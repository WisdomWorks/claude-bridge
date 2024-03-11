package com.example.demo.bridge;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;

public class JudgeList {
    private static final Logger logger = LoggerFactory.getLogger(JudgeList.class);

    private static final int PRIORITIES = 4;
    private static final int REJUDGE_PRIORITY = 2; // Adjust according to your needs

    private final ConcurrentLinkedQueue<Object> queue;
    private final ConcurrentHashMap<Long, Judge> submissionMap;
    private final ConcurrentHashMap<Long, Object> nodeMap;
    private final Set<JudgeHandler> judges;
    private final ReentrantLock lock;

    public JudgeList() {
        this.queue = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < PRIORITIES; i++) {
            queue.add(new PriorityMarker(i));
        }
        this.submissionMap = new ConcurrentHashMap<>();
        this.nodeMap = new ConcurrentHashMap<>();
        this.judges = new HashSet<>();
        this.lock = new ReentrantLock();
    }

    private void handleFreeJudge(Judge judge) {
        lock.lock();
        try {
            Object node = queue.peek();
            int priority = 0;
            while (node != null) {
                if (node instanceof PriorityMarker) {
                    priority = ((PriorityMarker) node).getPriority() + 1;
                } else if (priority >= REJUDGE_PRIORITY && countNotDisabled() > 1 &&
                        judges.stream().filter(j -> !j.isWorking() && !j.isDisabled()).count() <= 1) {
                    return;
                } else {
                    SubmissionData submissionData = (SubmissionData) node;
                    if (judge.canJudge(submissionData.getProblem(), submissionData.getLanguage(), submissionData.getJudgeId())) {
                        submissionMap.put(submissionData.getId(), judge);
                        try {
                            judge.submit(submissionData.getId(), submissionData.getProblem(), submissionData.getLanguage(), submissionData.getSource());
                        } catch (Exception e) {
                            logger.error("Failed to dispatch {} ({}, {}) to {}", submissionData.getId(), submissionData.getProblem(), submissionData.getLanguage(), judge.getName(), e);
                            judges.remove(judge);
                            return;
                        }
                        logger.info("Dispatched queued submission {}: {}", submissionData.getId(), judge.getName());
                        queue.remove(node);
                        nodeMap.remove(submissionData.getId());
                        break;
                    }
                }
                node = queue.peek();
            }
        } finally {
            lock.unlock();
        }
    }

    private int countNotDisabled() {
        return (int) judges.stream().filter(judge -> !judge.isDisabled()).count();
    }

    public void register(Judge judge) {
        lock.lock();
        try {
            // Disconnect all judges with the same name
            disconnect(judge.getName(), true);
            judges.add(judge);
            handleFreeJudge(judge);
        } finally {
            lock.unlock();
        }
    }

    public void disconnect(String judgeId, boolean force) {
        lock.lock();
        try {
            for (Judge judge : judges) {
                if (judge.getName().equals(judgeId)) {
                    judge.disconnect(force);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public void updateProblems(Judge judge) {
        lock.lock();
        try {
            handleFreeJudge(judge);
        } finally {
            lock.unlock();
        }
    }

    public void updateDisableJudge(String judgeId, boolean isDisabled) {
        lock.lock();
        try {
            for (Judge judge : judges) {
                if (judge.getName().equals(judgeId)) {
                    judge.setDisabled(isDisabled);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public void remove(Judge judge) {
        lock.lock();
        try {
            Long submission = judge.getCurrentSubmission();
            if (submission != null) {
                submissionMap.remove(submission);
            }
            judges.remove(judge);

            // Since we reserve a judge for high priority submissions when there are more than one,
            // we'll need to start judging if there is exactly one judge and it's free.
            if (judges.size() == 1) {
                Judge availableJudge = judges.iterator().next();
                if (!availableJudge.isWorking()) {
                    handleFreeJudge(availableJudge);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public Set<Judge> getJudges() {
        return judges;
    }

    public void onJudgeFree(Judge judge, long submission) {
        logger.info("Judge available after grading {}: {}", submission, judge.getName());
        lock.lock();
        try {
            submissionMap.remove(submission);
            judge.setWorking(false);
            handleFreeJudge(judge);
        } finally {
            lock.unlock();
        }
    }

    public boolean abort(long submission) {
        logger.info("Abort request: {}", submission);
        lock.lock();
        try {
            Judge judge = submissionMap.get(submission);
            if (judge != null) {
                judge.abort();
                return true;
            } else {
                Object node = nodeMap.remove(submission);
                if (node != null) {
                    queue.remove(node);
                }
                return false;
            }
        } finally {
            lock.unlock();
        }
    }

    public boolean checkPriority(int priority) {
        return priority >= 0 && priority < PRIORITIES;
    }

    public void judge(long id, String problem, String language, String source, String judgeId, int priority) {
        lock.lock();
        try {
            if (submissionMap.containsKey(id) || nodeMap.containsKey(id)) {
                // Already judging, don't queue again. This can happen during batch rejudges, rejudges should be idempotent.
                return;
            }

            Set<Judge> candidates = judges.stream().filter(judge -> judge.canJudge(problem, language, judgeId)).collect(toSet());
            Set<Judge> available = candidates.stream().filter(judge -> !judge.isWorking() && !judge.isDisabled()).collect(toSet());

            if (judgeId != null) {
                logger.info("Specified judge {} is{}available", judgeId, available.isEmpty() ? " not " : " ");
            } else {
                logger.info("Free judges: {}", available.size());
            }

            if (candidates.size() > 1 && available.size() == 1 && priority >= REJUDGE_PRIORITY) {
                available.clear();
            }

            if (!available.isEmpty()) {
                // Schedule the submission on the judge reporting least load.
                Judge judge = available.stream().min(Comparator.comparingDouble(j -> j.getLoad() + Math.random())).get();
                logger.info("Dispatched submission {} to: {}", id, judge.getName());
                submissionMap.put(id, judge);
                try {
                    judge.submit(id, problem, language, source);
                } catch (Exception e) {
                    logger.error("Failed to dispatch {} ({}, {}) to {}", id, problem, language, judge.getName(), e);
                    judges.remove(judge);
                    judge(id, problem, language, source, judgeId, priority);
                }
            } else {
                SubmissionData submissionData = new SubmissionData(id, problem, language, source, judgeId);
                nodeMap.put(id, submissionData);
                queue.add(submissionData);
                logger.info("Queued submission: {}", id);
            }
        } finally {
            lock.unlock();
        }
    }

    private static class PriorityMarker {
        private final int priority;

        public PriorityMarker(int priority) {
            this.priority = priority;
        }

        public int getPriority() {
            return priority;
        }
    }

    private static class SubmissionData {
        private final long id;
        private final String problem;
        private final String language;
        private final String source;
        private final String judgeId;

        public SubmissionData(long id, String problem, String language, String source, String judgeId) {
            this.id = id;
            this.problem = problem;
            this.language = language;
            this.source = source;
            this.judgeId = judgeId;
        }

        public long getId() {
            return id;
        }

        public String getProblem() {
            return problem;
        }

        public String getLanguage() {
            return language;
        }

        public String getSource() {
            return source;
        }

        public String getJudgeId() {
            return judgeId;
        }
    }
}
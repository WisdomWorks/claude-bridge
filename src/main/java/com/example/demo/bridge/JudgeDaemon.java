package com.example.demo.bridge;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Component
public class JudgeDaemon implements CommandLineRunner {
    private static final Logger logger = LoggerFactory.getLogger(JudgeDaemon.class);

    @Value("${bridged.judge.address}")
    private String bridgedJudgeAddress;

    @Value("${bridged.django.address}")
    private String bridgedDjangoAddress;

    private final JudgeService judgeService;
    private final SubmissionService submissionService;

    public JudgeDaemon(JudgeService judgeService, SubmissionService submissionService) {
        this.judgeService = judgeService;
        this.submissionService = submissionService;
    }

    @Override
    public void run(String... args) {
        resetJudges();
        updateSubmissions();

        JudgeList judges = new JudgeList();

        Server judgeServer = new Server(bridgedJudgeAddress, new JudgeHandler(judges));
        Server djangoServer = new Server(bridgedDjangoAddress, new SpringHandler(judges));

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.submit(judgeServer::serveForever);
        executorService.submit(djangoServer::serveForever);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Exiting due to shutdown signal");
            judgeServer.shutdown();
            djangoServer.shutdown();
            executorService.shutdown();
            try {
                executorService.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.error("Error while waiting for executor service to terminate", e);
            }
        }));
    }

    private void resetJudges() {
        judgeService.resetJudges();
    }

    private void updateSubmissions() {
        submissionService.updateInProgressSubmissions();
    }
}
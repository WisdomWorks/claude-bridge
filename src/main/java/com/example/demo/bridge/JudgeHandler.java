package com.example.demo.bridge;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class JudgeHandler extends TextWebSocketHandler {
    private static final Logger logger = LoggerFactory.getLogger(JudgeHandler.class);
    private static final Logger jsonLog = LoggerFactory.getLogger("judge.json.bridge");
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final JudgeService judgeService;
    private final Map<String, WebSocketSession> judges = new ConcurrentHashMap<>();
    private final ScheduledExecutorService pingExecutor = new ScheduledThreadPoolExecutor(1);

    private WebSocketSession session;
    private String judgeName;
    private boolean isWorking;
    private Long workingSubmissionId;

    public JudgeHandler(JudgeService judgeService) {
        this.judgeService = judgeService;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        this.session = session;
        logger.info("Judge connected from: {}", session.getRemoteAddress());
        jsonLog.info(makeJsonLog("connect"));
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
        pingExecutor.shutdown();
        judges.remove(judgeName);
        if (judgeName != null) {
            judgeService.disconnectJudge(judgeName);
        }
        logger.info("Judge disconnected from: {} with name {}", session.getRemoteAddress(), judgeName);
        jsonLog.info(makeJsonLog("disconnect", "judge disconnected"));
        if (isWorking) {
            judgeService.handleSubmissionError(workingSubmissionId, "IE", "IE", "");
            jsonLog.error(makeJsonLog("close", "IE due to shutdown on grading", workingSubmissionId));
        }
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        String payload = message.getPayload();
        Map<String, Object> packet = parsePacket(payload);

        if (packet != null) {
            String packetName = (String) packet.get("name");
            switch (packetName) {
                case "handshake":
                    handleHandshake(packet);
                    break;
                case "supported-problems":
                    handleSupportedProblems(packet);
                    break;
                case "grading-begin":
                    handleGradingBegin(packet);
                    break;
                case "grading-end":
                    handleGradingEnd(packet);
                    break;
                case "compile-error":
                    handleCompileError(packet);
                    break;
                case "compile-message":
                    handleCompileMessage(packet);
                    break;
                case "batch-begin":
                    handleBatchBegin(packet);
                    break;
                case "batch-end":
                    handleBatchEnd(packet);
                    break;
                case "test-case-status":
                    handleTestCaseStatus(packet);
                    break;
                case "internal-error":
                    handleInternalError(packet);
                    break;
                case "submission-terminated":
                    handleSubmissionTerminated(packet);
                    break;
                case "ping-response":
                    handlePingResponse(packet);
                    break;
                default:
                    handleMalformed(packet);
                    break;
            }
        } else {
            handleMalformed(payload);
        }
    }

    private Map<String, Object> parsePacket(String payload) {
        try {
            return objectMapper.readValue(payload, Map.class);
        } catch (IOException e) {
            logger.error("Error parsing packet", e);
            return null;
        }
    }

    private void handleHandshake(Map<String, Object> packet) {
        String judgeId = (String) packet.get("id");
        String key = (String) packet.get("key");

        if (authenticate(judgeId, key)) {
            judgeName = judgeId;
            judges.put(judgeName, session);
            pingExecutor.scheduleAtFixedRate(this::ping, 0, 10, TimeUnit.SECONDS);
            judgeService.connectJudge(judgeName);
            sendResponse(Map.of("name", "handshake-success"));
            logger.info("Judge authenticated: {} ({})", session.getRemoteAddress(), judgeId);
            jsonLog.info(makeJsonLog("auth", "judge successfully authenticated"));
        } else {
            logger.warn("Judge authentication failure: {}", session.getRemoteAddress());
            jsonLog.warn(makeJsonLog("auth", "judge failed authentication", judgeId));
            session.close();
        }
    }

    private boolean authenticate(String judgeId, String key) {
        // Implement judge authentication logic here
        // Example using HMAC-SHA256:
        try {
            String secretKey = "your-secret-key";
            Mac mac = Mac.getInstance("HmacSHA256");
            SecretKeySpec secretKeySpec = new SecretKeySpec(secretKey.getBytes(), "HmacSHA256");
            mac.init(secretKeySpec);
            byte[] hashBytes = mac.doFinal(judgeId.getBytes());
            String computedHash = Base64.getEncoder().encodeToString(hashBytes);
            return computedHash.equals(key);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            logger.error("Error during judge authentication", e);
            return false;
        }
    }

    private void handleSupportedProblems(Map<String, Object> packet) {
        // Update supported problems for the judge
        // Implement the logic to update the judge's supported problems in the database
        jsonLog.info(makeJsonLog("update-problems", String.valueOf(packet.get("problems").wait())));
    }

    private void handleGradingBegin(Map<String, Object> packet) {
        Long submissionId = Long.parseLong(packet.get("submission-id").toString());
        isWorking = true;
        workingSubmissionId = submissionId;
        // Update submission status to "G" (Grading) in the database
        // Delete existing test case records for the submission
        jsonLog.info(makeJsonLog("grading-begin", packet));
    }

    private void handleGradingEnd(Map<String, Object> packet) {
        Long submissionId = Long.parseLong(packet.get("submission-id").toString());
        isWorking = false;
        workingSubmissionId = null;
        // Update submission status, result, score, time, memory, etc. in the database
        // Update user statistics and problem statistics
        // Post grading-end event
        jsonLog.info(makeJsonLog("grading-end", packet));
    }

    private void handleCompileError(Map<String, Object> packet) {
        Long submissionId = Long.parseLong(packet.get("submission-id").toString());
        String errorMessage = (String) packet.get("log");
        isWorking = false;
        workingSubmissionId = null;
        // Update submission status to "CE" (Compile Error) and save the error message in the database
        // Post compile-error event
        jsonLog.info(makeJsonLog("compile-error", packet));
    }

    private void handleCompileMessage(Map<String, Object> packet) {
        Long submissionId = Long.parseLong(packet.get("submission-id").toString());
        String compileMessage = (String) packet.get("log");
        // Update submission with the compile message in the database
        // Post compile-message event
        jsonLog.info(makeJsonLog("compile-message", packet));
    }

    private void handleBatchBegin(Map<String, Object> packet) {
        // Handle batch begin logic
        jsonLog.info(makeJsonLog("batch-begin", packet));
    }

    private void handleBatchEnd(Map<String, Object> packet) {
        // Handle batch end logic
        jsonLog.info(makeJsonLog("batch-end", packet));
    }

    private void handleTestCaseStatus(Map<String, Object> packet) {
        Long submissionId = Long.parseLong(packet.get("submission-id").toString());
        // Update test case results in the database
        // Post test-case event
        jsonLog.info(makeJsonLog("test-case", packet));
    }

    private void handleInternalError(Map<String, Object> packet) {
        Long submissionId = Long.parseLong(packet.get("submission-id").toString());
        String errorMessage = (String) packet.get("message");
        isWorking = false;
        workingSubmissionId = null;
        // Update submission status to "IE" (Internal Error) and save the error message in the database
        // Post internal-error event
        jsonLog.info(makeJsonLog("internal-error", packet));
    }

    private void handleSubmissionTerminated(Map<String, Object> packet) {
        Long submissionId = Long.parseLong(packet.get("submission-id").toString());
        isWorking = false;
        workingSubmissionId = null;
        // Update submission status to "AB" (Aborted) in the database
        // Post aborted event
        jsonLog.info(makeJsonLog("aborted", packet));
    }

    private void handlePingResponse(Map<String, Object> packet) {
        // Update judge's ping, load, and time delta values
        // Implement the logic to update the judge's statistics in the database
    }

    private void handleMalformed(Map<String, Object> packet) {
        logger.error("Malformed packet: {}", packet);
        jsonLog.error(makeJsonLog("malformed json packet"));
    }

    private void handleMalformed(String payload) {
        logger.error("Malformed payload: {}", payload);
        jsonLog.error(makeJsonLog("malformed json payload"));
    }

    private void sendResponse(Map<String, Object> response) {
        try {
            String responseJson = objectMapper.writeValueAsString(response);
            session.sendMessage(new TextMessage(responseJson));
        } catch (IOException e) {
            logger.error("Error sending response", e);
        }
    }

    private void ping() {
        sendResponse(Map.of("name", "ping", "when", System.currentTimeMillis()));
    }

    private String makeJsonLog(String action) {
        return makeJsonLog(action, null, null);
    }

    private String makeJsonLog(String action, String info) {
        return makeJsonLog(action, info, null);
    }

    private String makeJsonLog(String action, Map<String, Object> packet) {
        Long submissionId = packet != null ? Long.parseLong(packet.get("submission-id").toString()) : null;
        return makeJsonLog(action, null, submissionId);
    }

    private String makeJsonLog(String action, String info, Long submissionId) {
        Map<String, Object> logData = Map.of(
                "judge", judgeName,
                "address", session.getRemoteAddress().toString(),
                "submission", submissionId,
                "action", action,
                "info", info
        );
        try {
            return objectMapper.writeValueAsString(logData);
        } catch (JsonProcessingException e) {
            logger.error("Error creating JSON log", e);
            return "";
        }
    }
}
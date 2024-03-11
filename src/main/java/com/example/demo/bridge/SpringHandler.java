package com.example.demo.bridge;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.util.Map;

public class SpringHandler extends TextWebSocketHandler {
    private static final Logger logger = LoggerFactory.getLogger(DjangoHandler.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final JudgeService judgeService;

    public SpringHandler(JudgeService judgeService) {
        this.judgeService = judgeService;
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws IOException {
        String payload = message.getPayload();
        Map<String, Object> packet = objectMapper.readValue(payload, Map.class);

        try {
            String packetName = (String) packet.get("name");
            Map<String, Object> result;

            switch (packetName) {
                case "submission-request":
                    result = onSubmission(packet);
                    break;
                case "terminate-submission":
                    result = onTermination(packet);
                    break;
                case "disconnect-judge":
                    onDisconnectRequest(packet);
                    result = Map.of();
                    break;
                case "disable-judge":
                    onDisableJudge(packet);
                    result = Map.of();
                    break;
                default:
                    result = onMalformed(packet);
            }

            sendResponse(session, result);
        } catch (Exception e) {
            logger.error("Error in packet handling (Django-facing)", e);
            sendResponse(session, Map.of("name", "bad-request"));
        }
    }

    private void sendResponse(WebSocketSession session, Map<String, Object> response) throws IOException {
        String responseJson = objectMapper.writeValueAsString(response);
        session.sendMessage(new TextMessage(responseJson));
    }

    private Map<String, Object> onSubmission(Map<String, Object> data) {
        Long id = (Long) data.get("submission-id");
        Long problem = (Long) data.get("problem-id");
        String language = (String) data.get("language");
        String source = (String) data.get("source");
        String judgeId = (String) data.get("judge-id");
        Integer priority = (Integer) data.get("priority");

        if (!judgeService.checkPriority(priority)) {
            return Map.of("name", "bad-request");
        }

        judgeService.judge(id, problem, language, source, judgeId, priority);
        return Map.of("name", "submission-received", "submission-id", id);
    }

    private Map<String, Object> onTermination(Map<String, Object> data) {
        Long submissionId = (Long) data.get("submission-id");
        boolean judgeAborted = judgeService.abort(submissionId);
        return Map.of("name", "submission-received", "judge-aborted", judgeAborted);
    }

    private void onDisconnectRequest(Map<String, Object> data) {
        String judgeId = (String) data.get("judge-id");
        boolean force = (boolean) data.get("force");
        judgeService.disconnect(judgeId, force);
    }

    private void onDisableJudge(Map<String, Object> data) {
        String judgeId = (String) data.get("judge-id");
        boolean isDisabled = (boolean) data.get("is-disabled");
        judgeService.updateDisableJudge(judgeId, isDisabled);
    }

    private Map<String, Object> onMalformed(Map<String, Object> packet) {
        logger.error("Malformed packet: {}", packet);
        return Map.of("name", "bad-request");
    }
}
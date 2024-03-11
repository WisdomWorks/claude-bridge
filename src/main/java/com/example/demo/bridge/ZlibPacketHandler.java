package com.example.demo.bridge;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.BinaryMessage;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.BinaryWebSocketHandler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class ZlibPacketHandler extends BinaryWebSocketHandler {
    private static final Logger logger = LoggerFactory.getLogger(ZlibPacketHandler.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final int MAX_ALLOWED_PACKET_SIZE = 8 * 1024 * 1024;
    private static final int SIZE_PACK_SIZE = 4;

    private WebSocketSession session;
    private InetSocketAddress clientAddress;
    private InetSocketAddress serverAddress;
    private byte[] initialTag;
    private boolean gotPacket;

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        this.session = session;
        this.clientAddress = session.getRemoteAddress();
        this.serverAddress = session.getLocalAddress();
        this.initialTag = null;
        this.gotPacket = false;
        onConnect();
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
        onDisconnect();
        onCleanup();
    }

    @Override
    protected void handleBinaryMessage(WebSocketSession session, BinaryMessage message) throws Exception {
        ByteBuffer payload = message.getPayload();
        byte[] data = new byte[payload.remaining()];
        payload.get(data);

        if (initialTag == null) {
            initialTag = Arrays.copyOf(data, SIZE_PACK_SIZE);
            data = Arrays.copyOfRange(data, SIZE_PACK_SIZE, data.length);
        }

        try {
            int size = ByteBuffer.wrap(data).getInt();
            if (size > MAX_ALLOWED_PACKET_SIZE) {
                logger.warn("Disconnecting client due to too-large message size ({} bytes): {}", size, clientAddress);
                throw new DisconnectException();
            }

            byte[] decompressedData = decompress(Arrays.copyOfRange(data, SIZE_PACK_SIZE, data.length));
            String decompressedString = new String(decompressedData);
            gotPacket = true;
            onPacket(decompressedString);
        } catch (DisconnectException e) {
            session.close();
        } catch (DataFormatException e) {
            if (gotPacket) {
                logger.warn("Encountered zlib error during packet handling, disconnecting client: {}", clientAddress, e);
            } else {
                logger.info("Potentially wrong protocol (zlib error): {}: {}", clientAddress, Arrays.toString(initialTag), e);
            }
            session.close();
        } catch (IOException e) {
            logger.error("Error handling binary message", e);
            session.close();
        }
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) throws Exception {
        if (gotPacket) {
            logger.info("Socket error: {}", clientAddress, exception);
            onTimeout();
        } else {
            logger.info("Potentially wrong protocol: {}: {}", clientAddress, Arrays.toString(initialTag));
        }
    }

    protected void onPacket(String data) {
        // Override this method to handle the received packet data
    }

    protected void onConnect() {
        // Override this method to handle the connection event
    }

    protected void onDisconnect() {
        // Override this method to handle the disconnection event
    }

    protected void onTimeout() {
        // Override this method to handle the timeout event
    }

    protected void onCleanup() {
        // Override this method to handle the cleanup event
    }

    protected void send(String data) throws IOException {
        byte[] compressedData = compress(data.getBytes());
        ByteBuffer buffer = ByteBuffer.allocate(SIZE_PACK_SIZE + compressedData.length);
        buffer.putInt(compressedData.length);
        buffer.put(compressedData);
        buffer.flip();
        session.sendMessage(new BinaryMessage(buffer));
    }

    protected void close() throws IOException {
        session.close();
    }

    private byte[] compress(byte[] data) throws IOException {
        Deflater deflater = new Deflater();
        deflater.setInput(data);
        deflater.finish();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        while (!deflater.finished()) {
            int count = deflater.deflate(buffer);
            outputStream.write(buffer, 0, count);
        }
        outputStream.close();
        return outputStream.toByteArray();
    }

    private byte[] decompress(byte[] data) throws IOException, DataFormatException {
        Inflater inflater = new Inflater();
        inflater.setInput(data);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        while (!inflater.finished()) {
            int count = inflater.inflate(buffer);
            outputStream.write(buffer, 0, count);
        }
        outputStream.close();
        return outputStream.toByteArray();
    }

    private static class DisconnectException extends Exception {
        // Custom exception to indicate a disconnect event
    }
}
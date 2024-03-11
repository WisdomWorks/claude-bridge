package com.example.demo.bridge;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Server {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    private final List<ServerSocket> serverSockets;
    private final List<ExecutorService> executorServices;
    private final Handler handler;

    public Server(List<InetSocketAddress> addresses, Handler handler) {
        this.serverSockets = new ArrayList<>();
        this.executorServices = new ArrayList<>();
        this.handler = handler;

        for (InetSocketAddress address : addresses) {
            try {
                ServerSocket serverSocket = new ServerSocket();
                serverSocket.setReuseAddress(true);
                serverSocket.bind(address);
                serverSockets.add(serverSocket);
                executorServices.add(Executors.newCachedThreadPool());
            } catch (IOException e) {
                logger.error("Failed to create server socket", e);
            }
        }
    }

    public void serveForever() {
        for (int i = 0; i < serverSockets.size(); i++) {
            ServerSocket serverSocket = serverSockets.get(i);
            ExecutorService executorService = executorServices.get(i);

            executorService.execute(() -> {
                while (!serverSocket.isClosed()) {
                    try {
                        Socket socket = serverSocket.accept();
                        executorService.execute(() -> handler.handle(socket));
                    } catch (IOException e) {
                        if (!serverSocket.isClosed()) {
                            logger.error("Failed to accept client connection", e);
                        }
                    }
                }
            });
        }
    }

    public void shutdown() {
        for (ServerSocket serverSocket : serverSockets) {
            try {
                serverSocket.close();
            } catch (IOException e) {
                logger.error("Failed to close server socket", e);
            }
        }

        for (ExecutorService executorService : executorServices) {
            executorService.shutdown();
        }
    }

    public interface Handler {
        void handle(Socket socket);
    }
}

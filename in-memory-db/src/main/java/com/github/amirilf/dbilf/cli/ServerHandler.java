package com.github.amirilf.dbilf.cli;

import java.net.ServerSocket;
import java.net.Socket;

public class ServerHandler {

    private int port;
    private boolean running;

    public ServerHandler(int port) {
        this.port = port;
    }

    public void start() {
        running = true;
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("DB Server started on port " + port);
            while (running) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("A client connected...");
                new Thread(new ClientHandler(clientSocket)).start();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void stop() {
        running = false;
    }
}

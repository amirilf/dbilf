package com.github.amirilf.dbilf;

import com.github.amirilf.dbilf.cli.ServerHandler;

public class App {
    public static void main(String[] args) {
        int port = 9090;
        ServerHandler server = new ServerHandler(port);
        server.start();
    }
}

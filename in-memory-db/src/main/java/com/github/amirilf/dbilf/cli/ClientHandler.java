package com.github.amirilf.dbilf.cli;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import com.github.amirilf.dbilf.query.QueryEngine;

public class ClientHandler implements Runnable {

    private Socket clientSocket;

    public ClientHandler(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }

    @Override
    public void run() {
        try (
                BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {

            out.println("<<DBILF>> Type 'exit' to disconnect.");
            out.print("dbilf> ");
            out.flush();
            String line;
            while ((line = in.readLine()) != null) {
                if (line.equalsIgnoreCase("exit")) {
                    out.println("Goodbye!");
                    break;
                }
                String result = QueryEngine.execute(line);
                out.println(result);
                out.print("dbilf> ");
                out.flush();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                clientSocket.close();
            } catch (Exception e) {
            }
        }
    }
}

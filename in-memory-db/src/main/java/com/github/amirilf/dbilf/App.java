package com.github.amirilf.dbilf;

import java.util.Scanner;
import com.github.amirilf.dbilf.cli.DatabaseServer;
import com.github.amirilf.dbilf.query.QueryEngine;

public class App {
    public static void main(String[] args) {
        // havin multiple sessions (each session is a new thread)
        if (args.length > 0 && args[0].equalsIgnoreCase("server")) {
            int port = 9090;
            if (args.length > 1) {
                port = Integer.parseInt(args[1]);
            }
            DatabaseServer server = new DatabaseServer(port);
            server.start();
        } else {
            // local
            Scanner scanner = new Scanner(System.in);
            System.out.println("<<DBILF>>\n'exit' to disconnect.");
            while (true) {
                System.out.print("> ");
                String command = scanner.nextLine();
                if (command.equalsIgnoreCase("exit")) {
                    break;
                }
                String result = QueryEngine.execute(command);
                System.out.println(result);
            }
            scanner.close();
        }
    }
}
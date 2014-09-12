package org.apache.pig.backend.hadoop.executionengine.spark;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

/* TCPServer thread to serve missed Objects */

public class BroadCastServer extends Thread {

    private ServerSocket serverSocket;
    private static Map<Object, Object> storage = null;

    int port;

    public BroadCastServer() {
    }

    public BroadCastServer(int port) throws IOException {

        this.port = port;
        serverSocket = new ServerSocket(port);

    }

    public void run() {
        while (true) {
            try {

                System.out.println("Waiting for client on port "
                        + serverSocket.getLocalPort() + "...");

                Socket server = serverSocket.accept();
                System.out.println("Just connected to "
                        + server.getRemoteSocketAddress());
                DataInputStream in = new DataInputStream(
                        server.getInputStream());
                String request = in.readUTF();
                System.out.println("Executor asking for :" + request);
                ObjectOutputStream out = new ObjectOutputStream(
                        server.getOutputStream());

                if (storage.get(request) != null) {

                    out.writeObject(storage.get(request));

                } else {

                    out.writeUTF("Requested resource not available!!");

                }

                server.close();

            } catch (Exception s) {
                s.printStackTrace();
                break;
            }
        }
    }

    public void startBroadcastServer(int port) {

        try {

            storage = new HashMap<Object, Object>();
            Thread t = new BroadCastServer(port);
            t.start();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void addResource(String reference, Object resource) {

        storage.put(reference, resource);

    }

}

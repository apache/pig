package org.apache.pig.backend.hadoop.executionengine.spark;

import java.net.Socket;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.OutputStream;

/* TCPClient to receive Objects from TCPServer */

public class BroadCastClient {

    private String host;
    private int port;

    public BroadCastClient(String host, int port) {

        this.host = host;
        this.port = port;

    }

    public Object getBroadCastMessage(String request) {

        Object response = null;

        try {

            System.out.println("Connecting to " + host + " on port " + port);
            Socket client = new Socket(host, port);

            OutputStream outToServer = client.getOutputStream();
            DataOutputStream out = new DataOutputStream(outToServer);

            out.writeUTF(request);
            ObjectInputStream inFromServer = new ObjectInputStream(
                    client.getInputStream());

            response = inFromServer.readObject();
            System.out.println("Server says " + response);
            client.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return response;
    }
}

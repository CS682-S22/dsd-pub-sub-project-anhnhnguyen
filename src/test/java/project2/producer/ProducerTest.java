package project2.producer;

import org.junit.jupiter.api.Test;
import project2.Constants;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class ProducerTest {

    @Test
    void testSuccessSimpleConnection() {
        String topic = "test";
        String key = "key";
        byte[] data = "this is a test".getBytes(StandardCharsets.UTF_8);
        Thread t = new Thread(() -> {
            try {
                ServerSocket serverSocket = new ServerSocket(1024);
                Socket socket = serverSocket.accept();
                DataInputStream dis = new DataInputStream(socket.getInputStream());
                int length = dis.readShort();
                if (length > 0) {
                    byte[] message = new byte[length];
                    dis.readFully(message, 0, length);
                    PubReq pubReq = new PubReq(message);
                    assertEquals(message[0], Constants.PUB_REQ);
                    assertEquals(topic, pubReq.getTopic());
                    assertEquals(key, pubReq.getKey());
                    assertArrayEquals(data, pubReq.getData());
                }
                dis.close();
                socket.close();
                serverSocket.close();
            } catch (IOException e) {
                fail(e.getMessage());
            }
        });
        t.start();
        Producer producer = new Producer("localhost", 1024);
        producer.send(topic, key, data);
        producer.close();
        try {
            t.join();
        } catch (InterruptedException e) {
            fail(e.getMessage());
        }
    }

    @Test
    void testSuccessComplexConnection() {
        String topic = "test";
        String key = "key";
        byte[] data = "this is a test".getBytes(StandardCharsets.UTF_8);
        Thread t = new Thread(() -> {
            try {
                ServerSocket serverSocket = new ServerSocket(1024);
                Socket socket = serverSocket.accept();
                DataInputStream dis = new DataInputStream(socket.getInputStream());
                int length = dis.readShort();
                while (length > 0) {
                    byte[] message = new byte[length];
                    dis.readFully(message, 0, length);
                    PubReq pubReq = new PubReq(message);
                    if (new String(pubReq.getData(), StandardCharsets.UTF_8).equals("FIN")) {
                        break;
                    }
                    assertEquals(message[0], Constants.PUB_REQ);
                    assertEquals(topic, pubReq.getTopic());
                    assertEquals(key, pubReq.getKey());
                    assertArrayEquals(data, pubReq.getData());
                    length = dis.readShort();
                }
                dis.close();
                socket.close();
                serverSocket.close();
            } catch (IOException e) {
                fail(e.getMessage());
            }
        });
        t.start();
        Producer producer = new Producer("localhost", 1024);
        for (int i = 0; i < 100000; i++) {
            producer.send(topic, key, data);
        }
        producer.send(topic, key, "FIN".getBytes(StandardCharsets.UTF_8));
        producer.close();
        try {
            t.join();
        } catch (InterruptedException e) {
            fail(e.getMessage());
        }
    }
}
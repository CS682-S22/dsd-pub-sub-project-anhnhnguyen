package project2.producer;

import org.junit.jupiter.api.Test;
import project2.protos.Message;

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
        byte[] data = "this is a test".getBytes(StandardCharsets.UTF_8);
        Thread t = new Thread(() -> {
            try {
                ServerSocket serverSocket = new ServerSocket(1024);
                Socket socket = serverSocket.accept();
                DataInputStream dis = new DataInputStream(socket.getInputStream());
                int length = dis.readInt();
                if (length > 0) {
                    byte[] message = new byte[length];
                    dis.readFully(message, 0, length);
                    Message.Wrapper wrapper = Message.Wrapper.parseFrom(message);
                    Message.PublishRequest pubReq = wrapper.getPubReq();
                    assertEquals(topic, pubReq.getTopic());
                    assertArrayEquals(data, pubReq.getData().toByteArray());
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
        producer.send(topic, data);
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
        byte[] data = "this is a test".getBytes(StandardCharsets.UTF_8);
        Thread t = new Thread(() -> {
            try {
                ServerSocket serverSocket = new ServerSocket(1024);
                Socket socket = serverSocket.accept();
                DataInputStream dis = new DataInputStream(socket.getInputStream());
                int length = dis.readInt();
                while (length > 0) {
                    byte[] message = new byte[length];
                    dis.readFully(message, 0, length);
                    Message.Wrapper wrapper = Message.Wrapper.parseFrom(message);
                    Message.PublishRequest pubReq = wrapper.getPubReq();
                    if (new String(pubReq.getData().toByteArray(), StandardCharsets.UTF_8).equals("FIN")) {
                        break;
                    }
                    assertEquals(topic, pubReq.getTopic());
                    assertArrayEquals(data, pubReq.getData().toByteArray());
                    length = dis.readInt();
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
            producer.send(topic, data);
        }
        producer.send(topic, "FIN".getBytes(StandardCharsets.UTF_8));
        producer.close();
        try {
            t.join();
        } catch (InterruptedException e) {
            fail(e.getMessage());
        }
    }
}
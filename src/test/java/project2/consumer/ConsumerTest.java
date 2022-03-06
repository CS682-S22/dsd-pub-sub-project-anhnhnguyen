package project2.consumer;

import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class ConsumerTest {

    @Test
    void testConsumerTimeout() {
        Thread t = new Thread(() -> {
            try {
                ServerSocket serverSocket = new ServerSocket(1024);
                Socket socket = serverSocket.accept();
                Thread.sleep(1000);
                socket.close();
                serverSocket.close();
            } catch (IOException | InterruptedException e) {
                fail(e.getMessage());
            }
        });
        t.start();
        Consumer consumer = new Consumer("localhost", 1024, "test", 0);

        long currentTime = System.currentTimeMillis();
        long endTime = currentTime + 500;
        byte[] message = consumer.poll(10);
        while (System.currentTimeMillis() < endTime) {
            message = consumer.poll(10);
        }

        assertNull(message);
        try {
            t.join();
        } catch (InterruptedException e) {
            fail(e.getMessage());
        }
        consumer.close();
    }

    @Test
    void testConsumerReceivingMessage() {
        Thread t = new Thread(() -> {
            try {
                ServerSocket serverSocket = new ServerSocket(1024);
                Socket socket = serverSocket.accept();
                DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                if (socket.getInputStream().read() != -1) {
                    for (int i = 0; i < 100; i++) {
                        Thread.sleep(20);
                        dos.writeInt("test".getBytes(StandardCharsets.UTF_8).length);
                        dos.write("test".getBytes(StandardCharsets.UTF_8));
                    }
                    dos.writeInt(-1);
                }
                dos.close();
                socket.close();
                serverSocket.close();
            } catch (IOException | InterruptedException e) {
                fail(e.getMessage());
            }
        });
        t.start();
        Consumer consumer = new Consumer("localhost", 1024, "test", 0);
        int count = 0;
        while (count < 100) {
            byte[] message = consumer.poll(10);
            if (message != null) {
                count++;
            }
        }
        assertEquals(count, 100);
        try {
            t.join();
        } catch (InterruptedException e) {
            fail(e.getMessage());
        }
        consumer.close();
    }
}
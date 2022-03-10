package project2.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import project2.Constants;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.Queue;

public class Consumer {
    private final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);
    private Socket socket;
    private final String topic;
    private long startingPosition;
    private DataInputStream dis;
    private DataOutputStream dos;
    private final Queue<byte[]> queue;

    public Consumer(String host, int port, String topic, long startingPosition) {
        this.topic = topic;
        this.startingPosition = startingPosition;
        this.queue = new LinkedList<>();
        try {
            this.socket = new Socket(host, port);
            LOGGER.info("open connection with broker: " + host + ":" + port);
            this.dis = new DataInputStream(socket.getInputStream());
            this.dos = new DataOutputStream(socket.getOutputStream());
        } catch (IOException e) {
            LOGGER.error("can't open connection with broker: " + host + ":" + port + " " + e.getMessage());
        }
    }

    private void send(String topic, long startingPosition) {
        try {
            dos.writeShort(topic.getBytes(StandardCharsets.UTF_8).length + 10);
            dos.writeByte(Constants.PULL_REQ);
            dos.write(topic.getBytes(StandardCharsets.UTF_8));
            dos.writeByte(0);
            dos.writeLong(startingPosition);
            LOGGER.info("pull request sent. topic: " + topic + ", starting position: " + startingPosition);
        } catch (IOException e) {
            LOGGER.error("send(): " + e.getMessage());
        }
    }

    public byte[] poll(int milliseconds) {
        if (!queue.isEmpty()) {
            return queue.poll();
        }
        send(topic, startingPosition);
        try {
            socket.setSoTimeout(milliseconds);
            int length = dis.readShort();
            while (length > 0) {
                LOGGER.info("received message from: " + socket.getRemoteSocketAddress());
                byte[] message = new byte[length];
                dis.readFully(message, 0, length);
                queue.add(message);
                startingPosition++;
                length = dis.readShort();
            }
        } catch (SocketTimeoutException e) {
            // do nothing
        } catch (IOException e) {
            LOGGER.error("poll(): " + e.getMessage());
        }
        return queue.poll();
    }

    public void close() {
        try {
            socket.shutdownInput();
            socket.shutdownOutput();
            socket.close();
            dis.close();
            dos.close();
            LOGGER.info("closing consumer");
        } catch (IOException e) {
            LOGGER.error("closer(): " + e.getMessage());
        }
    }
}

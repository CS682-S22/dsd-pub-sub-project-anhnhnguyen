package project2.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import project2.Constants;
import project2.protos.Message;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.LinkedList;
import java.util.Queue;

public class Consumer {
    private final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);
    private Socket socket;
    private final String topic;
    private int startingPosition;
    private DataInputStream dis;
    private DataOutputStream dos;
    private final Queue<byte[]> queue;

    public Consumer(String host, int port, String topic, int startingPosition) {
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

    private void send(String topic, int startingPosition) {
        Message.PullRequest pullReq = Message.PullRequest.newBuilder()
                .setTopic(topic)
                .setStartingPosition(startingPosition)
                .build();
        Message.Wrapper wrapper = Message.Wrapper.newBuilder()
                .setPullReq(pullReq)
                .build();
        try {
            dos.writeInt(wrapper.toByteArray().length);
            dos.write(wrapper.toByteArray());
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
            int length = dis.readInt();
            while (length > 0) {
                byte[] message = new byte[length];
                dis.readFully(message, 0, length);
                if (new String(message).equals(Constants.INVALID_TOPIC) ||
                        new String(message).equals(Constants.INVALID_STARTING_POSITION) ) {
                    return message;
                }
                queue.add(message);
                startingPosition++;
                length = dis.readInt();
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

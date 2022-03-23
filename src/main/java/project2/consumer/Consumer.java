package project2.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import project2.Constants;
import project2.broker.ReqRes;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Class that pulls message from the Broker by specifying topic and starting position to pull.
 *
 * @author anhnguyen
 */
public class Consumer {
    /**
     * logger object.
     */
    private final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);
    /**
     * topic.
     */
    protected final String topic;
    /**
     * starting position.
     */
    protected long startingPosition;
    /**
     * partition.
     */
    protected final int partition;
    /**
     * socket.
     */
    private Socket socket;
    /**
     * data input stream.
     */
    private DataInputStream dis;
    /**
     * data output stream.
     */
    protected DataOutputStream dos;
    /**
     * message queue.
     */
    private final Queue<byte[]> queue;
    /**
     * timer.
     */
    private final Timer timer;

    /**
     * Constructor.
     *
     * @param host             host
     * @param port             port
     * @param topic            topic
     * @param startingPosition starting position
     * @param partition        partition
     */
    public Consumer(String host, int port, String topic, long startingPosition, int partition) {
        try {
            this.socket = new Socket(host, port);
            LOGGER.info("open connection with broker: " + host + ":" + port);
            this.dis = new DataInputStream(this.socket.getInputStream());
            this.dos = new DataOutputStream(this.socket.getOutputStream());
        } catch (IOException e) {
            LOGGER.error("can't open connection with broker: " + host + ":" + port + " " + e.getMessage());
        }
        this.topic = topic;
        this.startingPosition = startingPosition;
        this.partition = partition;
        this.queue = new LinkedList<>();
        this.timer = new Timer();
    }

    /**
     * method to periodically send request to broker every specified milliseconds if no responses from broker.
     *
     * @param milliseconds interval
     * @return byte[] array of message received
     */
    public byte[] poll(int milliseconds) {
        byte[] message = getMessage(milliseconds);
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                try {
                    byte[] request = prepareRequest(topic, startingPosition, (byte) Constants.PULL_REQ, partition, Constants.NUM_RESPONSE);
                    dos.writeShort(request.length);
                    dos.write(request);
                    LOGGER.info("pull request sent. topic: " + topic + ", partition: " + partition + ", starting position: " + startingPosition);
                } catch (IOException e) {
                    LOGGER.error("poll(): " + e.getMessage());
                }
            }
        };
        timer.schedule(task, milliseconds);
        return message;
    }

    /**
     * Method to prepare request to broker to pull message for the specified topic and starting position.
     *
     * @param topic            topic
     * @param startingPosition starting position
     * @param messageType      message type (Pull request or subscribe request)
     * @return byte array in the form of [1-byte message type] | [topic] | 0 | [8-byte offset] | [2-byte partition] | [2-byte num messages]
     */
    protected byte[] prepareRequest(String topic, long startingPosition, byte messageType, int partition, int numMessages) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(topic.getBytes(StandardCharsets.UTF_8).length + 14);
        byteBuffer.put(messageType);
        byteBuffer.put(topic.getBytes(StandardCharsets.UTF_8));
        byteBuffer.put((byte) 0);
        byteBuffer.putLong(startingPosition);
        byteBuffer.putShort((short) partition);
        byteBuffer.putShort((short) numMessages);
        return byteBuffer.array();
    }

    /**
     * Method to get message from the connection and verify message is not duplicate.
     *
     * @return byte array
     */
    protected byte[] getMessage(int milliseconds) {
        if (!queue.isEmpty()) {
            return queue.poll();
        }
        try {
            socket.setSoTimeout(milliseconds);
            int length = dis.readShort();
            while (length > 0) {
                LOGGER.info("received message from: " + socket.getRemoteSocketAddress());
                byte[] message = new byte[length];
                dis.readFully(message, 0, length);
                ReqRes response = new ReqRes(message);
                if (response.getOffset() >= startingPosition) {
                    queue.add(message);
                    synchronized ((Long) startingPosition) {
                        startingPosition = response.getOffset() + response.getKey().getBytes(StandardCharsets.UTF_8).length
                                + response.getData().length + 1;
                    }
                }
                length = dis.readShort();
            }
        } catch (SocketTimeoutException e) {
            // do nothing
        } catch (IOException e) {
            LOGGER.error("getMessage(): " + e.getMessage());
        }
        return queue.poll();
    }

    /**
     * method to close consumer.
     */
    public void close() {
        try {
            timer.cancel();
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

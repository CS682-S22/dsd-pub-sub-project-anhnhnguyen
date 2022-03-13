package project2.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import project2.Constants;
import project2.Connection;
import project2.broker.ReqRes;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

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
     * socket.
     */
    private AsynchronousSocketChannel socket;
    /**
     * topic.
     */
    private final String topic;
    /**
     * starting position.
     */
    private long startingPosition;
    /**
     * the queue to read messages from broker before delivering up to the application.
     */
    private final Queue<byte[]> queue;
    /**
     * connection object.
     */
    private final Connection connection;

    /**
     * Constructor.
     *
     * @param topic            topic
     * @param startingPosition starting position
     */
    public Consumer(String host, int port, String topic, long startingPosition) {
        try {
            this.socket = AsynchronousSocketChannel.open();
            Future<Void> future = socket.connect(new InetSocketAddress(host, port));
            future.get();
            LOGGER.info("opening socket channel connecting with: " + host + ":" + port);
        } catch (IOException | ExecutionException | InterruptedException e) {
            LOGGER.error("can't open socket channel");
        }
        this.topic = topic;
        this.startingPosition = startingPosition;
        this.queue = new LinkedList<>();
        this.connection = new Connection(socket);
    }

    /**
     * Method to prepare request to broker to pull message for the specified topic and starting position.
     *
     * @param topic            topic
     * @param startingPosition starting position
     * @return byte array in the form of [1-byte message type] | [topic] | 0 | [8-byte offset]
     */
    private byte[] prepareRequest(String topic, long startingPosition) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(topic.getBytes(StandardCharsets.UTF_8).length + 10);
        byteBuffer.put((byte) Constants.PULL_REQ);
        byteBuffer.put(topic.getBytes(StandardCharsets.UTF_8));
        byteBuffer.put((byte) 0);
        byteBuffer.putLong(startingPosition);
        return byteBuffer.array();
    }

    /**
     * method to periodically send request to broker every specified milliseconds if no responses from broker.
     *
     * @param milliseconds interval
     * @return byte[] array of message received
     */
    public byte[] poll(int milliseconds) {
        if (!queue.isEmpty()) {
            return queue.poll();
        }
        connection.send(prepareRequest(topic, startingPosition));
        LOGGER.info("pull request sent. topic: " + topic + ", starting position: " + startingPosition);
        byte[] message = connection.receive(milliseconds);
        while (message != null) {
            queue.add(message);
            ReqRes response = new ReqRes(message);
            startingPosition = response.getOffset() + response.getKey().getBytes(StandardCharsets.UTF_8).length
                    + response.getData().length + 1;
            message = connection.receive(milliseconds);
        }
        return queue.poll();
    }

    /**
     * Method to close consumer.
     */
    public void close() {
        try {
            socket.shutdownOutput();
            socket.shutdownInput();
            socket.close();
            LOGGER.info("closing consumer");
        } catch (IOException e) {
            LOGGER.error("closer(): " + e.getMessage());
        }
    }
}

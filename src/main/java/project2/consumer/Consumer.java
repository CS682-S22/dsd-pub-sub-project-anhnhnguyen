package project2.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import project2.Constants;
import project2.broker.ReqRes;
import project2.zookeeper.BrokerMetadata;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Class that pulls message from the Broker by specifying topic and starting position to pull.
 *
 * @author anhnguyen
 */
public class Consumer extends ConsumerDriver {
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
    protected volatile long startingPosition;
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
    protected final Queue<byte[]> queue;
    /**
     * scheduler.
     */
    protected ScheduledExecutorService scheduler;
    /**
     * host.
     */
    private String host;
    /**
     * port.
     */
    private int port;

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
        this.host = host;
        this.port = port;
        this.topic = topic;
        this.startingPosition = startingPosition;
        this.partition = partition;
        this.queue = new LinkedList<>();
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        // thread to periodically send a request to pull data and populate the queue where application polls from
        this.scheduler.scheduleWithFixedDelay(this::request, 0, Constants.INTERVAL, TimeUnit.MILLISECONDS);
    }

    /**
     * method to periodically send request to broker every specified milliseconds if no responses from broker.
     *
     * @param milliseconds time wait while queue is empty
     * @return byte[] array of message received
     */
    public byte[] poll(int milliseconds) {
        synchronized (this) {
            if (queue.isEmpty()) {
                try {
                    wait(milliseconds);
                } catch (InterruptedException e) {
                    LOGGER.error("poll(): " + e.getMessage());
                }
            }
        }
        return queue.poll();
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
     * Method to make a pull request and fill the queue with response to be consumed by application.
     * If detecting host failure, go to Zookeeper to find a new host and reroute the request to the new host.
     */
    private void request() {
        try {
            byte[] request = prepareRequest(topic, startingPosition, (byte) Constants.PULL_REQ, partition, Constants.NUM_RESPONSE);
            dos.writeShort(request.length);
            dos.write(request);
            dos.flush();
            LOGGER.info("pull request sent. topic: " + topic + ", partition: " + partition + ", starting position: " + startingPosition);
            getMessage();
        } catch (IOException e) {
            LOGGER.error("poll(): " + e.getMessage());
            scheduler.shutdown();
            Collection<BrokerMetadata> brokers = curator.findBrokers();
            BrokerMetadata broker = findBroker(brokers, partition);
            while (broker == null || broker.getListenAddress().equals(host) && broker.getListenPort() == port) {
                synchronized (this) {
                    try {
                        wait(Constants.TIME_OUT);
                    } catch (InterruptedException exc) {
                        LOGGER.error("wait(): " + exc.getMessage());
                    }
                }
                LOGGER.info("Looking for new broker");
                brokers = curator.findBrokers();
                broker = findBroker(brokers, partition);
            }
            try {
                host = broker.getListenAddress();
                port = broker.getListenPort();
                socket = new Socket(broker.getListenAddress(), broker.getListenPort());
                dis = new DataInputStream(socket.getInputStream());
                dos = new DataOutputStream(socket.getOutputStream());
                scheduler = Executors.newSingleThreadScheduledExecutor();
                scheduler.scheduleWithFixedDelay(this::request, 0, Constants.INTERVAL, TimeUnit.MILLISECONDS);
            } catch (IOException exc) {
                LOGGER.error(exc.getMessage());
            }
        }
    }

    /**
     * Method to get message from the connection and verify message is not duplicate.
     */
    protected void getMessage() throws IOException {
        try {
            socket.setSoTimeout(Constants.TIME_OUT);
            int length = dis.readShort();
            while (length > 0) {
                LOGGER.info("received message from: " + socket.getRemoteSocketAddress());
                byte[] message = new byte[length];
                dis.readFully(message, 0, length);
                ReqRes response = new ReqRes(message);
                if (response.getOffset() >= startingPosition) {
                    queue.add(message);
                    startingPosition = response.getOffset() + response.getKey().getBytes(StandardCharsets.UTF_8).length
                            + response.getData().length + 1;
                }
                length = dis.readShort();
            }
        } catch (SocketTimeoutException e) {
            // do nothing
        }
    }

    /**
     * method to close consumer.
     */
    public void close() {
        try {
            scheduler.shutdownNow();
            if (!scheduler.awaitTermination(Constants.TIME_OUT, TimeUnit.MILLISECONDS)) {
                LOGGER.error("awaitTermination()");
            }
            socket.shutdownInput();
            socket.shutdownOutput();
            socket.close();
            dis.close();
            dos.close();
            LOGGER.info("closing consumer");
        } catch (IOException | InterruptedException e) {
            LOGGER.error("closer(): " + e.getMessage());
        }
    }
}

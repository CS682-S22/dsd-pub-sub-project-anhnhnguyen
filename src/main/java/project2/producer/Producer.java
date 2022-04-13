package project2.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import project2.Connection;
import project2.Constants;
import project2.Utils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Class that sends message to broker.
 *
 * @author anhnguyen
 */
public class Producer {
    /**
     * logger object.
     */
    private final Logger LOGGER = LoggerFactory.getLogger("operation");
    /**
     * socket.
     */
    private AsynchronousSocketChannel socket;
    /**
     * connection object.
     */
    private final Connection connection;
    /**
     * host.
     */
    private final String host;
    /**
     * port.
     */
    private final int port;

    /**
     * Constructor.
     *
     * @param host host
     * @param port port
     */
    public Producer(String host, int port) {
        this.host = host;
        this.port = port;
        try {
            this.socket = AsynchronousSocketChannel.open();
            Future<Void> future = this.socket.connect(new InetSocketAddress(host, port));
            future.get();
            LOGGER.info("opening socket channel connecting with: " + host + ":" + port);
        } catch (IOException | ExecutionException | InterruptedException e) {
            LOGGER.error("can't open socket channel: " + e.getMessage());
        }
        this.connection = new Connection(socket);
    }

    /**
     * method to publish topic, key, and data to broker.
     *
     * @param topic         topic
     * @param key           key
     * @param data          data
     * @param numPartitions number of partitions
     * @return true if sending successfully and receiving ack else false
     */
    public boolean send(String topic, String key, byte[] data, int numPartitions) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(key.getBytes(StandardCharsets.UTF_8).length + 1 + data.length);
        byteBuffer.put(key.getBytes(StandardCharsets.UTF_8));
        byteBuffer.put((byte) 0);
        byteBuffer.put(data);
        try {
            connection.send(Utils.preparePubReq(byteBuffer.array(), topic, (short) numPartitions).array());
            byte[] ack = connection.receive();
            int count = 0;
            while ((ack == null || ack[0] != Constants.ACK_RES) && count < Constants.RETRY) {
                ack = connection.receive();
                count++;
            }
            if (ack == null || ack[0] != Constants.ACK_RES) {
                LOGGER.info("suspect broker failure");
                return false;
            }
            LOGGER.info("message sent. topic: " + topic + ", key: "
                    + key + ", data: " + new String(data, StandardCharsets.UTF_8));
            return true;
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOGGER.error("IOException: " + e.getMessage());
            close();
            return false;
        }
    }

    /**
     * method to close socket.
     */
    public void close() {
        try {
            socket.shutdownInput();
            socket.shutdownOutput();
            socket.close();
            LOGGER.info("closing producer");
        } catch (IOException e) {
            LOGGER.error("close(): " + e.getMessage());
        }
    }

    /**
     * Getter for host.
     *
     * @return host
     */
    public String getHost() {
        return host;
    }

    /**
     * Getter for port.
     *
     * @return port
     */
    public int getPort() {
        return port;
    }
}

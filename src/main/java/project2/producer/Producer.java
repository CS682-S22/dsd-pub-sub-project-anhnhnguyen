package project2.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import project2.Constants;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * Class that sends message to broker.
 *
 * @author anhnguyen
 */
public class Producer {
    /**
     * logger object.
     */
    private final Logger LOGGER = LoggerFactory.getLogger(Producer.class);
    /**
     * socket.
     */
    private Socket socket;
    /**
     * data output stream.
     */
    private DataOutputStream dos;

    /**
     * Constructor.
     *
     * @param host host
     * @param port port
     */
    public Producer(String host, int port) {
        try {
            this.socket = new Socket(host, port);
            LOGGER.info("open connection with broker: " + host + ":" + port);
            this.dos = new DataOutputStream(socket.getOutputStream());
        } catch (IOException e) {
            LOGGER.error("can't open connection with broker: " + host + ":" + port + " " + e.getMessage());
        }
    }

    /**
     * method to publish topic, key, and data to broker.
     *
     * @param topic topic
     * @param key   key
     * @param data  data
     */
    public void send(String topic, String key, byte[] data) {
        try {
            int length = topic.getBytes(StandardCharsets.UTF_8).length
                    + key.getBytes(StandardCharsets.UTF_8).length + data.length + 3;
            dos.writeShort(length);
            dos.writeByte(Constants.PUB_REQ);
            dos.write(topic.getBytes(StandardCharsets.UTF_8));
            dos.writeByte(0);
            dos.write(key.getBytes(StandardCharsets.UTF_8));
            dos.writeByte(0);
            dos.write(data);
            LOGGER.info("message sent. topic: " + topic + ", key: "
                    + key + ", data: " + new String(data, StandardCharsets.UTF_8));
        } catch (IOException e) {
            LOGGER.error("send(): " + e.getMessage());
        }
    }

    /**
     * method to close producer.
     */
    public void close() {
        try {
            socket.shutdownInput();
            socket.shutdownOutput();
            socket.close();
            dos.close();
            LOGGER.info("closing producer");
        } catch (IOException e) {
            LOGGER.error("close(): " + e.getMessage());
        }
    }
}

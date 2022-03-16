package project2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * A super class of producer and consumer that open socket to connect with broker and close socket when done.
 *
 * @author anhnguyen
 */
public abstract class Client {
    /**
     * logger object.
     */
    private final Logger LOGGER = LoggerFactory.getLogger(Client.class);
    /**
     * socket.
     */
    private AsynchronousSocketChannel socket;
    /**
     * connection object.
     */
    protected final Connection connection;

    /**
     * Constructor.
     *
     * @param host host
     * @param port port
     */
    protected Client(String host, int port) {
        try {
            this.socket = AsynchronousSocketChannel.open();
            Future<Void> future = socket.connect(new InetSocketAddress(host, port));
            future.get();
            LOGGER.info("opening socket channel connecting with: " + host + ":" + port);
        } catch (IOException | ExecutionException | InterruptedException e) {
            LOGGER.error("can't open socket channel: " + e.getMessage());
        }
        this.connection = new Connection(socket);
    }

    /**
     * method to close socket.
     */
    public void close() {
        try {
            socket.shutdownInput();
            socket.shutdownOutput();
            socket.close();
            LOGGER.info("closing socket");
        } catch (IOException e) {
            LOGGER.error("close(): " + e.getMessage());
        }
    }
}

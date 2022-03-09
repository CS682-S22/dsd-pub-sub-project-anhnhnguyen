package project2.broker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import project2.Constants;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Class to establish a connection to send and receive messages over socket.
 *
 * @author anhnguyen
 * <p>
 * Reference: https://seb-nyberg.medium.com/length-delimited-protobuf-streams-a39ebc4a4565
 * Reference: https://medium.com/@dmi3coder/c-java-socket-communication-easily-lets-try-protobuf-586b18521f79
 */
public class Connection {
    /**
     * the logger object.
     */
    private final Logger LOGGER = LoggerFactory.getLogger(Connection.class);
    /**
     * the socket channel.
     */
    private final AsynchronousSocketChannel socketChannel;
    /**
     * the message queue for receiving message.
     */
    private final Queue<byte[]> messages;
    /**
     * the buffer to read message.
     */
    private final ByteBuffer buffer;
    /**
     * the future object to read buffer.
     */
    private Future<Integer> readResult;

    /**
     * Constructor.
     *
     * @param socketChannel the socket channel
     */
    public Connection(AsynchronousSocketChannel socketChannel) {
        this.socketChannel = socketChannel;
        this.messages = new LinkedList<>();
        this.buffer = ByteBuffer.allocate(Constants.BYTE_ALLOCATION);
        this.readResult = null;
    }

    /**
     * Method to read message from remote host.
     *
     * @return byte array of the message
     */
    public byte[] receive() {
        if (!messages.isEmpty()) {
            return messages.poll();
        }
        if (socketChannel != null && socketChannel.isOpen()) {
            if (readResult == null) {
                readResult = socketChannel.read(buffer);
            }
            try {
                if (readResult.get(500, TimeUnit.MILLISECONDS) != -1 && readResult.isDone()) {
                    readResult = null;
                    int size = buffer.position();
                    int count = 0;
                    buffer.flip(); // set the position to 0
                    while (count + 2 < size) { // need 2 bytes to read a short
                        int length = buffer.getShort();
                        if (count + length + 2 > size) { // message is partially read in due to buffer capacity
                            break;
                        }
                        byte[] bytes = new byte[length];
                        buffer.get(bytes, 0, length);
                        messages.add(bytes);
                        count += (length + 2);
                    }
                    if (count < size) { // bytes not read due to buffer capacity
                        buffer.position(count);
                        byte[] bytes = new byte[size - count];
                        buffer.get(bytes, 0, bytes.length);
                        buffer.clear();
                        buffer.put(bytes, 0, bytes.length);
                    } else {
                        buffer.clear();
                    }
                }
            } catch (InterruptedException | ExecutionException e) {
                LOGGER.error("receive(): " + e.getMessage());
            } catch (TimeoutException e) {
                // do nothing
            }
        }
        return messages.poll();
    }

    /**
     * Method to send byte array over the socket.
     *
     * @param message byte array
     */
    public void send(byte[] message) {
        try {
            if (socketChannel != null && socketChannel.isOpen()) {
                ByteBuffer buffer = ByteBuffer.allocate(message.length + 2);
                buffer.putShort((short) message.length);
                buffer.put(message);
                buffer.flip();
                Future<Integer> writeResult = socketChannel.write(buffer);
                writeResult.get();
            }
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error("send(byte[] message): " + e.getMessage());
        }
    }
}

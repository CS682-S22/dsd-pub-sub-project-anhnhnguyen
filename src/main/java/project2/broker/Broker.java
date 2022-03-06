package project2.broker;

import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import project2.protos.Message;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class Broker {
    private final Logger LOGGER = LoggerFactory.getLogger(Broker.class);
    private AsynchronousServerSocketChannel server;
    private volatile boolean isRunning;
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<byte[]>> topics;

    public Broker(String host, int port) {
        this.topics = new ConcurrentHashMap<>();
        try {
            this.server = AsynchronousServerSocketChannel.open().bind(new InetSocketAddress(host, port));
            LOGGER.info("broker started on: " + ((InetSocketAddress) server.getLocalAddress()).getHostName()
                    + ":" + ((InetSocketAddress) server.getLocalAddress()).getPort());
            this.isRunning = true;
        } catch (IOException e) {
            LOGGER.error("can't start broker on: " + host + ":" + port + " " + e.getMessage());
        }
    }

    public void start() {
        server.accept(null, new CompletionHandler<>() {
            @Override
            public void completed(AsynchronousSocketChannel result, Object attachment) {
                try {
                    LOGGER.info("connection from: " + ((InetSocketAddress) result.getRemoteAddress()).getHostName()
                            + ":" + ((InetSocketAddress) result.getRemoteAddress()).getPort());
                    if (server.isOpen()) {
                        server.accept(null, this);
                    }
                    Connection connection = new Connection(result);
                    while (isRunning) {
                        byte[] request = connection.receive();
                        if (request != null) {
                            processRequest(connection, request);
                        }
                    }
                    result.shutdownInput();
                    result.shutdownOutput();
                    result.close();
                } catch (IOException e) {
                    LOGGER.error("start(): " + e.getMessage());
                }
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                if (server.isOpen()) {
                    LOGGER.error("fail to accept connection");
                }
            }
        });
    }

    private void processRequest(Connection connection, byte[] request) {
        try {
            Message.Wrapper wrapper = Message.Wrapper.parseFrom(request);
            if (wrapper.hasPubReq()) {
                Message.PublishRequest pubReq = wrapper.getPubReq();
                String topic = pubReq.getTopic();
                byte[] data = pubReq.getData().toByteArray();
                LOGGER.info("publish request. topic: " + topic + ", data: " + new String(data, StandardCharsets.UTF_8));
                processPubReq(topic, data);
            }
            if (wrapper.hasPullReq()) {
                Message.PullRequest pullReq = wrapper.getPullReq();
                String topic = pullReq.getTopic();
                int startingPosition = pullReq.getStartingPosition();
                LOGGER.info("pull request. topic: " + topic + ", starting position: " + startingPosition);
                processPullReq(connection, topic, startingPosition);
            }
        } catch (InvalidProtocolBufferException e) {
            LOGGER.error("processRequest(): " + e.getMessage());
        }
    }

    private void processPubReq(String topic, byte[] data) {
        // Reference: https://stackoverflow.com/questions/18605876/concurrent-hashmap-and-copyonwritearraylist
        CopyOnWriteArrayList<byte[]> copy = topics.get(topic);
        if (copy == null) {
            copy = new CopyOnWriteArrayList<>();
            CopyOnWriteArrayList<byte[]> inMap = topics.putIfAbsent(topic, copy);
            if (inMap != null) {
                copy = inMap;
            }
        }
        copy.add(data);
        LOGGER.info("data added to topic: " + topic);
    }

    private void processPullReq(Connection connection, String topic, int startingPosition) {
        if (!topics.containsKey(topic)) {
            connection.send("Invalid request: topic doesn't exist".getBytes(StandardCharsets.UTF_8));
            LOGGER.info("Invalid request: topic: " + topic + " doesn't exist");
        } else {
            List<byte[]> list = topics.get(topic);
            if (startingPosition >= list.size()) {
                connection.send("Invalid request: starting position doesn't exist".getBytes(StandardCharsets.UTF_8));
                LOGGER.info("Invalid request: starting position: " + startingPosition + " doesn't exist");
            } else {
                while (startingPosition < list.size()) {
                    connection.send(list.get(startingPosition));
                    LOGGER.info("data at: " + startingPosition + " from topic: " + topic + " sent");
                    startingPosition++;
                }
            }
        }
    }

    public void close() {
        try {
            isRunning = false;
            server.close();
            LOGGER.info("closing broker");
        } catch (IOException e) {
            LOGGER.error("close(): " + e.getMessage());
        }
    }
}

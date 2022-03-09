package project2.broker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import project2.Constants;
import project2.consumer.PullReq;
import project2.producer.PubReq;

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
        if (request[0] == Constants.PUB_REQ) {
            PubReq pubReq = new PubReq(request);
            String topic = pubReq.getTopic();
            String key = pubReq.getKey();
            byte[] data = pubReq.getData();
            LOGGER.info("publish request. topic: " + topic + ", key: " + key +
                    ", data: " + new String(data, StandardCharsets.UTF_8));
            processPubReq(topic, key, data);
        }
        if (request[0] == Constants.PULL_REQ) {
            PullReq pullReq = new PullReq(request);
            String topic = pullReq.getTopic();
            long startingPosition = pullReq.getStartingPosition();
            LOGGER.info("pull request. topic: " + topic + ", starting position: " + startingPosition);
            processPullReq(connection, topic, startingPosition);
        }
    }

    private void processPubReq(String topic, String key, byte[] data) {
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

    private void processPullReq(Connection connection, String topic, long startingPosition) {
        if (topics.containsKey(topic)) {
            List<byte[]> list = topics.get(topic);
            if (startingPosition < list.size()) {
                int count = 0;
                while (startingPosition < list.size() && count < Constants.NUM_RESPONSE) {
                    connection.send(list.get((int) startingPosition));
                    LOGGER.info("data at: " + startingPosition + " from topic: " + topic + " sent");
                    startingPosition++;
                    count++;
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

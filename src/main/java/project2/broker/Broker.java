package project2.broker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import project2.Constants;
import project2.consumer.PullReq;
import project2.producer.PubReq;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
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
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<PubReq>> topics;

    public Broker(String host, int port) {
        this.topics = new ConcurrentHashMap<>();
        try {
            this.server = AsynchronousServerSocketChannel.open().bind(new InetSocketAddress(host, port));
            LOGGER.info("broker started on: " + server.getLocalAddress());
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
                    LOGGER.info("connection from: " + result.getRemoteAddress());
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
                    LOGGER.info("closing socket channel");
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
            processPubReq(topic, pubReq);
        }
        if (request[0] == Constants.PULL_REQ) {
            PullReq pullReq = new PullReq(request);
            String topic = pullReq.getTopic();
            long startingPosition = pullReq.getStartingPosition();
            LOGGER.info("pull request. topic: " + topic + ", starting position: " + startingPosition);
            processPullReq(connection, topic, startingPosition);
        }
    }

    private void processPubReq(String topic, PubReq pubReq) {
        // Reference: https://stackoverflow.com/questions/18605876/concurrent-hashmap-and-copyonwritearraylist
        CopyOnWriteArrayList<PubReq> copy = topics.get(topic);
        if (copy == null) {
            copy = new CopyOnWriteArrayList<>();
            CopyOnWriteArrayList<PubReq> inMap = topics.putIfAbsent(topic, copy);
            if (inMap != null) {
                copy = inMap;
            }
        }
        copy.add(pubReq);
        LOGGER.info("data added to topic: " + topic + ", key: " + pubReq.getKey());
    }

    private void processPullReq(Connection connection, String topic, long startingPosition) {
        if (topics.containsKey(topic)) {
            List<PubReq> list = topics.get(topic);
            if (startingPosition < list.size()) {
                int count = 0;
                while (startingPosition < list.size() && count < Constants.NUM_RESPONSE) {
                    PubReq pubReq = list.get((int) startingPosition);
                    int length = pubReq.getKey().getBytes(StandardCharsets.UTF_8).length + pubReq.getData().length + 10;
                    ByteBuffer response = ByteBuffer.allocate(length);
                    response.put((byte) Constants.REQ_RES);
                    response.putLong(startingPosition);
                    response.put(pubReq.getKey().getBytes(StandardCharsets.UTF_8));
                    response.put((byte) 0);
                    response.put(pubReq.getData());
                    connection.send(response.array());
                    LOGGER.info("data at: " + startingPosition + " from topic: " + topic + " sent");
                    startingPosition++;
                    count++;
                }
            }
        }
    }

    public void close() {
        try {
            LOGGER.info("closing broker");
            isRunning = false;
            server.close();
        } catch (IOException e) {
            LOGGER.error("close(): " + e.getMessage());
        }
    }
}

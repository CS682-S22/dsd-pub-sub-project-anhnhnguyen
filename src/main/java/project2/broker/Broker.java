package project2.broker;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import project2.Config;
import project2.Connection;
import project2.Constants;
import project2.Utils;
import project2.consumer.PullReq;
import project2.producer.PubReq;
import project2.zookeeper.BrokerMetadata;
import project2.zookeeper.BrokerRegister;
import project2.zookeeper.InstanceSerializerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Class that listens to request to publish message from Producer and request to pull message from Consumer.
 *
 * @author anhnguyen
 */
public class Broker {
    /**
     * logger object.
     */
    private final Logger LOGGER = LoggerFactory.getLogger(Broker.class);
    /**
     * server.
     */
    private AsynchronousServerSocketChannel server;
    /**
     * state of server.
     */
    private volatile boolean isRunning;
    /**
     * broker register.
     */
    private BrokerRegister brokerRegister;
    /**
     * lock map for connection.
     */
    private final Map<Connection, Lock> connectionLockMap;
    /**
     * thread pool.
     */
    private final ScheduledThreadPoolExecutor threadPool;
    /**
     * membership table.
     */
    private Member members;
    /**
     * leader status.
     */
    private boolean isLeader;
    /**
     * list of followers.
     */
    private final Map<BrokerMetadata, Connection> followers;
    /**
     * topic class.
     */
    private final Topic topicStruct;

    /**
     * Start the broker to listen on the given host name and port number. Also delete old log files
     * and create new folder (if necessary) at initialization (for testing purpose).
     *
     * @param config           config
     * @param curatorFramework curator framework
     * @param objectMapper     object mapper
     */
    public Broker(Config config, CuratorFramework curatorFramework, ObjectMapper objectMapper) {
        this.topicStruct = new Topic();
        this.connectionLockMap = new ConcurrentHashMap<>();
        this.isRunning = true;
        this.threadPool = new ScheduledThreadPoolExecutor(Constants.NUM_THREADS);
        this.isLeader = config.isLeader();
        this.followers = new HashMap<>();
        try {
            this.server = AsynchronousServerSocketChannel.open().bind(new InetSocketAddress(config.getHost(), config.getPort()));
            LOGGER.info("broker started on: " + server.getLocalAddress());
        } catch (IOException e) {
            LOGGER.error("can't start broker on: " + config.getHost() + ":" + config.getPort() + " " + e.getMessage());
        }
        if (isLeader) {
            this.brokerRegister = new BrokerRegister(curatorFramework,
                    new InstanceSerializerFactory(objectMapper.reader(), objectMapper.writer()),
                    Constants.SERVICE_NAME, config.getHost(), config.getPort(), config.getPartition(), config.getId());
            this.brokerRegister.registerAvailability();
        }
        threadPool.schedule(() -> {
            members = new Member(config);
        }, 0, TimeUnit.MILLISECONDS);
        Utils.deleteFiles(new File(Constants.LOG_FOLDER));
        Utils.createFolder(Constants.LOG_FOLDER);
    }

    /**
     * Method to start the server to accept incoming request and handle the request accordingly.
     */
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
                        try {
                            byte[] request = connection.receive();
                            if (request != null) {
                                processRequest(connection, request);
                            }
                        } catch (IOException | InterruptedException | ExecutionException e) {
                            LOGGER.error(e.getMessage());
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

    /**
     * Method to determine which request is which and route the request to the appropriate method to handle the request.
     *
     * @param connection socket connection
     * @param request    request
     */
    private void processRequest(Connection connection, byte[] request) {
        if (request[0] == Constants.PUB_REQ) {
            processPubReq(connection, request);
        } else if (request[0] == Constants.PULL_REQ) {
            processPullReq(connection, request);
        } else if (new String(request, StandardCharsets.UTF_8).equals(Constants.MEM)) {
            sendMembers(connection);
        } else if (request[0] == Constants.REP_REQ) {
            processRepReq(connection, request);
        } else {
            LOGGER.error("Invalid request: " + request[0]);
        }
    }

    /**
     * Method to process the publish request.
     * If this is the first time topic is published, then create 2 lists: offset list that holds all offsets id in the list
     * and starting offset list that holds just the offset of the first message in every segment files. Add 0 (starting offset) to both lists.
     * <p>
     * Add id of the next message to the offset list (id = current message id + current message length)
     * Write key value to the segment files in the tmp/ folder.
     * When the file is ~ the maximum allowed size file for segment file, then copy the file to the log/folder and write message to the next segment file.
     * Add offset of the message of the next segment file to the starting offset list.
     *
     * @param request    request
     * @param connection connection
     *                   <p>
     *                   Reference for locking on certain topic: https://stackoverflow.com/questions/71007235/java-synchronized-on-the-same-file-but-not-different
     */
    private void processPubReq(Connection connection, byte[] request) {
        PubReq pubReq = new PubReq(request);
        LOGGER.info("publish request. topic: " + pubReq.getTopic() + ", key: " + pubReq.getKey() +
                ", data: " + new String(pubReq.getData(), StandardCharsets.UTF_8));
        long current = topicStruct.updateTopic(pubReq);
        if (isLeader) {
            try {
                reconcileList();
                for (Connection followerConnection : followers.values()) {
                    ByteBuffer repReq = ByteBuffer.allocate(request.length + 9);
                    repReq.put((byte) Constants.REP_REQ);
                    repReq.putLong(current);
                    repReq.put(request);
                    followerConnection.send(repReq.array());
                    String ackMessage = "";
                    int count = 0;
                    while (!ackMessage.equals(Constants.ACK) && count < Constants.RETRY) {
                        byte[] ack = followerConnection.receive();
                        if (ack != null) {
                            ackMessage = new String(ack, StandardCharsets.UTF_8);
                        }
                        count++;
                    }
                }
            } catch (IOException | InterruptedException | ExecutionException e) {
                LOGGER.error(e.getMessage());
            }
        }
        try {
            LOGGER.info("sending ack to publish request");
            connection.send(Constants.ACK.getBytes(StandardCharsets.UTF_8));
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOGGER.error("processPubReq(): " + e.getMessage());
        }

    }

    /**
     * Method to first search for the index of the starting position in the offset list and the log file that has the
     * starting position. Then read key value from the file and send up to 10 messages to the consumer.
     *
     * @param connection connection
     * @param request    request
     */
    private void processPullReq(Connection connection, byte[] request) {
        PullReq pullReq = new PullReq(request);
        String topic = pullReq.getTopic();
        long startingPosition = pullReq.getStartingPosition();
        LOGGER.info("pull request. topic: " + topic + ", partition: " + pullReq.getPartition() + ", starting position: " + startingPosition);
        Map<String, Map<Integer, List<List<Long>>>> topics = topicStruct.getTopics();
        if (topics.containsKey(topic)) {
            List<List<Long>> list = topics.get(topic).get(pullReq.getPartition());
            if (list.size() == 2) {
                List<Long> offSetList = list.get(Constants.OFFSET_INDEX);
                List<Long> startingOffsetList = list.get(Constants.STARTING_OFFSET_INDEX);

                // search for the index of the offset in the offset list, excluding the last index because it belongs to the
                // future message
                int index = Arrays.binarySearch(offSetList.toArray(), startingPosition);
                if (index == offSetList.size() - 1) {
                    index = -1;
                } else if (index < 0 && startingPosition < offSetList.get(offSetList.size() - 1)) {
                    // if consumer sends a starting position that is not in the list,
                    // round it down to the nearest offset (for testing purpose to make sure starting position works)
                    index = -(index + 1) - 1;
                }
                if (index >= 0) {
                    int count = 0;
                    int numMessages = pullReq.getNumMessages();
                    if (numMessages == 0) {
                        numMessages = Integer.MAX_VALUE; // send during push (for first subscription)
                    }

                    while (index < offSetList.size() - 1 && count < numMessages) {
                        long offset = offSetList.get(index);
                        // search for the file that has the offset, binarySearch method include insertionPoint which is
                        // the index where the number would be put in if it doesn't find the number. So for this application
                        // return the lower index because that's where the byte offset would be.
                        int fileIndex = Arrays.binarySearch(startingOffsetList.toArray(), offset);
                        if (fileIndex < 0) {
                            fileIndex = -(fileIndex + 1) - 1;
                        }
                        String fileName = Constants.LOG_FOLDER + topic + Constants.PATH_STRING + pullReq.getPartition() +
                                Constants.PATH_STRING + startingOffsetList.get(fileIndex) + Constants.FILE_TYPE;
                        // only expose to consumer when data is flushed to disk, so need to check the log/ folder
                        if (Files.exists(Paths.get(fileName))) {
                            try (RandomAccessFile raf = new RandomAccessFile(fileName, "r")) {
                                long length = offSetList.get(index + 1) - offSetList.get(index);
                                long position = offSetList.get(index) - startingOffsetList.get(fileIndex);
                                sendData(connection, length, offset, position, topic, raf, pullReq.getPartition());
                                index++;
                                count++;
                            } catch (IOException e) {
                                LOGGER.error("processPullReq(): " + e.getMessage());
                            }
                        } else {
                            break;
                        }
                    }
                }
            }
        }
    }

    /**
     * Method to send data to consumer via connection by looking up the position of data in file and read data length.
     *
     * @param connection connection
     * @param length     length of message
     * @param position   position
     * @param offset     offset of message
     * @param partition  partition
     */
    private void sendData(Connection connection, long length, long offset, long position, String topic, RandomAccessFile raf, int partition) {
        try {
            byte[] data = new byte[(int) length];
            raf.seek(position);
            raf.read(data);
            ByteBuffer response = ByteBuffer.allocate((int) length + 9);
            response.put((byte) Constants.REQ_RES);
            response.putLong(offset);
            response.put(data);
            connection.send(response.array());
            LOGGER.info("data at offset: " + offset + " from topic: " + topic + ", partition: " + partition + " sent");
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOGGER.error("sendData(): " + e.getMessage());
        }
    }

    /**
     * Method to close the broker.
     */
    public void close() {
        try {
            LOGGER.info("closing broker");
            isRunning = false;
            threadPool.shutdown();
            if (!threadPool.awaitTermination(Constants.TIME_OUT, TimeUnit.MILLISECONDS)) {
                LOGGER.error("awaitTermination()");
            }
            for (Connection connection : followers.values()) {
                connection.close();
            }
            if (brokerRegister != null) {
                brokerRegister.unregisterAvailability();
            }
            members.close();
            server.close();
        } catch (IOException | InterruptedException e) {
            LOGGER.error("close(): " + e.getMessage());
        }
    }

    /**
     * Method to send member information to requesting host.
     *
     * @param connection connection
     */
    private void sendMembers(Connection connection) {
        try {
            if (members == null) {
                ByteBuffer resp = ByteBuffer.allocate(2);
                resp.putShort((short) (0));
                connection.send(resp.array());
                return;
            }
            BrokerMetadata leader = members.getLeader();
            Map<BrokerMetadata, Connection> followers = members.getFollowers();
            ByteBuffer resp = ByteBuffer.allocate(2);
            resp.putShort((short) (followers.keySet().size() + 1));
            connection.send(resp.array());
            if (leader == null) {
                resp = ByteBuffer.allocate(Constants.NONE.getBytes(StandardCharsets.UTF_8).length + 6);
                resp.putShort((short) 0);
                resp.putShort((short) 0);
                resp.putShort((short) 0);
                resp.put(Constants.NONE.getBytes(StandardCharsets.UTF_8));
                connection.send(resp.array());
            } else {
                connection.send(getBrokerInfo(leader));
            }
            for (BrokerMetadata follower : followers.keySet()) {
                connection.send(getBrokerInfo(follower));
            }
        } catch (IOException | ExecutionException | InterruptedException e) {
            LOGGER.error("sendMembers(): " + e.getMessage());
        }
    }

    /**
     * Method to get the byte array of the broker metadata.
     *
     * @param broker broker metadata
     * @return byte array
     */
    private byte[] getBrokerInfo(BrokerMetadata broker) {
        ByteBuffer resp = ByteBuffer.allocate(broker.getListenAddress().getBytes(StandardCharsets.UTF_8).length + 6);
        resp.putShort((short) broker.getListenPort());
        resp.putShort((short) broker.getPartition());
        resp.putShort((short) broker.getId());
        resp.put(broker.getListenAddress().getBytes(StandardCharsets.UTF_8));
        return resp.array();
    }

    /**
     * Method to process replication request.
     *
     * @param connection connection
     * @param request    request
     */
    private void processRepReq(Connection connection, byte[] request) {
        byte[] offsetBytes = Utils.extractBytes(1, 9, request, false);
        long offset = new BigInteger(offsetBytes).longValue();
        byte[] message = new byte[request.length - 9];
        System.arraycopy(request, 9, message, 0, message.length);
        PubReq pubReq = new PubReq(message);
        String topic = pubReq.getTopic();
        int partition = pubReq.getKey().hashCode() % pubReq.getNumPartitions();
        List<Long> offsetList = new ArrayList<>();
        Map<String, Map<Integer, List<List<Long>>>> topics = topicStruct.getTopics();
        if (topics.containsKey(topic) && topics.get(topic).containsKey(partition)) {
            offsetList = topics.get(topic).get(partition).get(Constants.OFFSET_INDEX);
        }
        if (offsetList.size() == 0 || offsetList.get(offsetList.size() - 1) == offset) {
            processPubReq(connection, message);
        }
    }

    /**
     * Method to reconcile list of followers based on member data structure.
     */
    private void reconcileList() {
        if (members == null) {
            return;
        }
        try {
            TreeMap<BrokerMetadata, Connection> followerList = members.getFollowers();
            for (BrokerMetadata follower : followerList.keySet()) {
                if (!followers.containsKey(follower)) {
                    AsynchronousSocketChannel socket = AsynchronousSocketChannel.open();
                    Future<Void> future = socket.connect(new InetSocketAddress(follower.getListenAddress(), follower.getListenPort()));
                    future.get();
                    Connection followerConnection = new Connection(socket);
                    followers.put(follower, followerConnection);
                    threadPool.execute(() -> sendSnapshot(followerConnection));
                }
            }
            for (BrokerMetadata follower : followers.keySet()) {
                if (!followerList.containsKey(follower)) {
                    followers.remove(follower);
                }
            }
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOGGER.error("reconcileList(): " + e.getMessage());
        }
    }

    /**
     * Method to send snapshot of the database to followers.
     *
     * @param connection connection
     */
    private void sendSnapshot(Connection connection) {
        Lock lock = connectionLockMap.computeIfAbsent(connection, l -> new ReentrantLock());
        lock.lock();
        try {
            Map<String, Map<Integer, List<List<Long>>>> topics = topicStruct.getTopics();
            Map<String, Map<Integer, List<byte[]>>> tmp = topicStruct.getTmp();
            for (String topic : topics.keySet()) {
                for (int partition : topics.get(topic).keySet()) {
                    List<Long> offSetList = topics.get(topic).get(partition).get(Constants.OFFSET_INDEX);
                    List<Long> startingOffsetList = topics.get(topic).get(partition).get(Constants.STARTING_OFFSET_INDEX);
                    int i;
                    for (i = 0; i < offSetList.size(); i++) {
                        long offset = offSetList.get(i);
                        int fileIndex = Arrays.binarySearch(startingOffsetList.toArray(), offset);
                        if (fileIndex < 0) {
                            fileIndex = -(fileIndex + 1) - 1;
                        }
                        String fileName = Constants.LOG_FOLDER + topic + Constants.PATH_STRING + partition +
                                Constants.PATH_STRING + startingOffsetList.get(fileIndex) + Constants.FILE_TYPE;
                        if (Files.exists(Paths.get(fileName))) {
                            try (RandomAccessFile raf = new RandomAccessFile(fileName, "r")) {
                                long length = offSetList.get(i + 1) - offSetList.get(i);
                                long position = offSetList.get(i) - startingOffsetList.get(fileIndex);
                                byte[] data = new byte[(int) length];
                                raf.seek(position);
                                raf.read(data);
                                ByteBuffer response = ByteBuffer.allocate((int) length + 9);
                                response.put((byte) Constants.REP_REQ);
                                response.putLong(offset);
                                response.put(data);
                                connection.send(response.array());
                                i++;
                            }
                        } else {
                            break;
                        }
                    }
                    int j = 0;
                    while (i < offSetList.size()) {
                        byte[] data = tmp.get(topic).get(partition).get(j);
                        ByteBuffer response = ByteBuffer.allocate(data.length + 9);
                        response.put((byte) Constants.REP_REQ);
                        response.putLong(offSetList.get(i));
                        response.put(data);
                        connection.send(response.array());
                        i++;
                        j++;
                    }
                }
            }
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOGGER.error("sendSnapshot(): " + e.getMessage());
        } finally {
            lock.unlock();
        }
    }
}

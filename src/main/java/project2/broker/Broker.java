package project2.broker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import project2.Config;
import project2.Connection;
import project2.Constants;
import project2.Utils;
import project2.broker.protos.Membership;
import project2.consumer.PullReq;
import project2.producer.PubReq;
import project2.zookeeper.BrokerMetadata;
import project2.zookeeper.BrokerRegister;
import project2.zookeeper.InstanceSerializerFactory;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Class that listens to request to publish message from Producer and request to pull message from Consumer.
 * If the broker is a leader, then replicate messages to followers before acknowledging messages to the Producer.
 * If the broker is a follower, then catching up with the leader then periodically polls from queue to persistent log.
 *
 * @author anhnguyen
 */
public class Broker {
    /**
     * logger object.
     */
    private final Logger LOGGER = LoggerFactory.getLogger("operation");
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
     * timer to schedule task.
     */
    private Timer timer;
    /**
     * membership table.
     */
    private Member members;
    /**
     * leader status.
     */
    private boolean isLeader;
    /**
     * list of async followers.
     */
    private final Map<BrokerMetadata, Connection> asyncFollowers;
    /**
     * list of sync followers.
     */
    private final Map<BrokerMetadata, Connection> syncFollowers;
    /**
     * topic class.
     */
    private final Topic topicStruct;
    /**
     * replication request queue.
     */
    private final Queue<byte[]> repQueue;
    /**
     * catching up state.
     */
    private volatile boolean isCatchingUp;
    /**
     * count of pub req from leader.
     */
    private int pubCount;
    /**
     * host.
     */
    private final String host;
    /**
     * port.
     */
    private final int port;
    /**
     * recent rep req.
     */
    private final List<byte[]> status;
    /**
     * curator framework.
     */
    private final CuratorFramework curatorFramework;
    /**
     * object mapper.
     */
    private final ObjectMapper objectMapper;

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
        this.isRunning = true;
        this.timer = new Timer();
        this.isLeader = config.isLeader();
        this.syncFollowers = new ConcurrentHashMap<>();
        this.asyncFollowers = new ConcurrentHashMap<>();
        this.repQueue = new ConcurrentLinkedQueue<>();
        this.isCatchingUp = true;
        this.pubCount = 0;
        this.host = config.getHost();
        this.port = config.getPort();
        this.status = new ArrayList<>();
        this.curatorFramework = curatorFramework;
        this.objectMapper = objectMapper;
        try {
            this.server = AsynchronousServerSocketChannel.open().bind(new InetSocketAddress(config.getHost(), config.getPort()));
            LOGGER.info("broker started on: " + server.getLocalAddress());
        } catch (IOException e) {
            LOGGER.error("can't start broker on: " + config.getHost() + ":" + config.getPort() + " " + e.getMessage());
        }

        if (this.isLeader) {
            this.brokerRegister = new BrokerRegister(curatorFramework,
                    new InstanceSerializerFactory(objectMapper.reader(), objectMapper.writer()),
                    Constants.SERVICE_NAME, config.getHost(), config.getPort(), config.getPartition(), config.getId(), config.isAsync());
            this.brokerRegister.registerAvailability();
            LOGGER.info("registering with Zookeeper");
        }

        this.timer.schedule(new TimerTask() {
            @Override
            public void run() {
                members = new Member(config, curatorFramework, objectMapper);
                LOGGER.info("set up members table");
            }
        }, 0);

        Utils.deleteFiles(new File(Constants.LOG_FOLDER));
        Utils.createFolder(Constants.LOG_FOLDER);

        if (!this.isLeader) {
            this.timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    removeCatchingUpStatus();
                }
            }, Constants.TIME_OUT * 5);

            this.timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    poll();
                }
            }, 0, Constants.INTERVAL);
        }
    }

    /**
     * Method to cancel the catching up status if there's no need to catch up.
     */
    private void removeCatchingUpStatus() {
        if (pubCount == 0) {
            isCatchingUp = false;
            LOGGER.info("No need to catch up");
        }
    }

    /**
     * Method to poll request from the queue and update the topic struct accordingly.
     */
    private void poll() {
        LOGGER.info("try polling");
        if (!isCatchingUp) {
            while (!repQueue.isEmpty()) {
                byte[] req = repQueue.poll();
                LOGGER.info("polling off replication queue");
                byte[] offsetBytes = Utils.extractBytes(1, 9, req, false);
                long offset = new BigInteger(offsetBytes).longValue();
                byte[] data = new byte[req.length - 9];
                System.arraycopy(req, 9, data, 0, data.length);
                PubReq pubReq = new PubReq(data);
                String topic = pubReq.getTopic();
                int partition = pubReq.getKey().hashCode() % pubReq.getNumPartitions();
                List<Long> offsetList = new ArrayList<>();
                if (topicStruct.getTopics().containsKey(topic) && topicStruct.getTopics().get(topic).containsKey(partition)) {
                    offsetList = topicStruct.getTopics().get(topic).get(partition).get(Constants.OFFSET_INDEX);
                }
                // only update the topic struct if the offset is expected, in case of duplicate
                if (offsetList.size() == 0 || offsetList.get(offsetList.size() - 1) == offset) {
                    topicStruct.updateTopic(pubReq);
                }
            }
        }
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
                            connection.close();
                            break;
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
            if (!isLeader) {
                pubCount++;
            }
            processPubReq(connection, request);
        } else if (request[0] == Constants.PULL_REQ) {
            processPullReq(connection, request);
        } else if (request[0] == Constants.MEM_REQ) {
            processMemReq(connection);
        } else if (request[0] == Constants.REP_REQ) {
            processRepReq(connection, request);
        } else if (request[0] == Constants.CAT_FIN) {
            processCatFin(connection);
        } else if (request[0] == Constants.ELECT_REQ) {
            processElectReq(connection);
        } else if (request[0] == Constants.VIC_MESS) {
            processVicMess(request);
        } else if (request[0] == Constants.REC_REQ) {
            processRecReq(connection);
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
    private synchronized void processPubReq(Connection connection, byte[] request) {
        PubReq pubReq = new PubReq(request);
        LOGGER.info("publish request. topic: " + pubReq.getTopic() + ", key: " + pubReq.getKey() +
                ", data: " + new String(pubReq.getData(), StandardCharsets.UTF_8));

        try {
            if (isLeader) {
                reconcileList();
                int partition = pubReq.getKey().hashCode() % pubReq.getNumPartitions();
                long current = 0;
                if (topicStruct.getTopics().containsKey(pubReq.getTopic()) && topicStruct.getTopics().get(pubReq.getTopic()).containsKey(partition)) {
                    List<List<Long>> indexes = topicStruct.getTopics().get(pubReq.getTopic()).get(partition);
                    current = indexes.get(Constants.OFFSET_INDEX).get(indexes.get(Constants.OFFSET_INDEX).size() - 1);
                }
                for (Connection followerConnection : syncFollowers.values()) {
                    LOGGER.info("Sending replication request to sync followers");
                    followerConnection.send(Utils.prepareData(Constants.REP_REQ, current, request).array());
                    followerConnection.waitForAck();
                }

                topicStruct.updateTopic(pubReq);

                LOGGER.info("sending ack to publish request");
                connection.send(new byte[]{(byte) Constants.ACK_RES});

                for (Connection followerConnection : asyncFollowers.values()) {
                    LOGGER.info("Sending replication request to async followers");
                    followerConnection.send(Utils.prepareData(Constants.REP_REQ, current, request).array());
                }
            } else {
                topicStruct.updateTopic(pubReq);

                LOGGER.info("sending ack to publish request");
                connection.send(new byte[]{(byte) Constants.ACK_RES});

            }
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOGGER.error(e.getMessage());
        }
    }

    /**
     * Method to reconcile list of followers based on member data structure.
     */
    private void reconcileList() {
        if (members == null) {
            return;
        }
        TreeMap<BrokerMetadata, Connection> followerList = new TreeMap<>(members.getFollowers());
        for (BrokerMetadata follower : followerList.keySet()) {
            if (!asyncFollowers.containsKey(follower) && follower.isAsync()) {
                updateListAddition(follower, true);
            } else if (!syncFollowers.containsKey(follower) && !follower.isAsync()) {
                updateListAddition(follower, false);
            }
        }
        updateListRemoval(followerList, true);
        updateListRemoval(followerList, false);
    }

    /**
     * Method to update async or sync list with new follower.
     *
     * @param follower new follower
     * @param isAsync  follower's async status
     */
    private void updateListAddition(BrokerMetadata follower, boolean isAsync) {
        try {
            AsynchronousSocketChannel socket = AsynchronousSocketChannel.open();
            Future<Void> future = socket.connect(new InetSocketAddress(follower.getListenAddress(), follower.getListenPort()));
            future.get();
            Connection followerConnection = new Connection(socket);
            if (isAsync) {
                asyncFollowers.put(follower, followerConnection);
                LOGGER.info("added to async follower list: " + follower.getId());
            } else {
                syncFollowers.put(follower, followerConnection);
                LOGGER.info("added to sync follower list: " + follower.getId());
            }

            Topic copy = new Topic(topicStruct);
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    sendSnapshot(follower, copy);
                }
            }, 0);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOGGER.error(e.getMessage());
        }
    }

    /**
     * Method to remove stale brokers from list.
     *
     * @param followerList follower list
     * @param isAsync      async status
     */
    private void updateListRemoval(TreeMap<BrokerMetadata, Connection> followerList, boolean isAsync) {
        Map<BrokerMetadata, Connection> followers;
        if (isAsync) {
            followers = asyncFollowers;
        } else  {
            followers = syncFollowers;
        }
        for (BrokerMetadata follower : followers.keySet()) {
            if (!followerList.containsKey(follower)) {
                followers.get(follower).close();
                followers.remove(follower);
                LOGGER.info("removed from follower list: " + follower.getId());
            }
        }
    }

    /**
     * Method to send snapshot of the database to followers in a separate thread and connection.
     *
     * @param follower follower
     * @param copy     copy of topic
     */
    private void sendSnapshot(BrokerMetadata follower, Topic copy) {
        try {
            AsynchronousSocketChannel socket = AsynchronousSocketChannel.open();
            Future<Void> future = socket.connect(new InetSocketAddress(follower.getListenAddress(), follower.getListenPort()));
            future.get();
            Connection connection = new Connection(socket);
            LOGGER.info("sending snapshot to: " + follower.getId());
            Map<String, Map<Integer, List<List<Long>>>> topics = copy.getTopics();
            Map<String, Map<Integer, List<byte[]>>> tmp = copy.getTmp();
            for (String topic : topics.keySet()) {
                for (int partition : topics.get(topic).keySet()) {
                    List<Long> offSetList = topics.get(topic).get(partition).get(Constants.OFFSET_INDEX);
                    List<Long> startingOffsetList = topics.get(topic).get(partition).get(Constants.STARTING_OFFSET_INDEX);
                    List<Long> numPartitionsList = topics.get(topic).get(partition).get(Constants.NUM_PARTITIONS_INDEX);
                    for (int i = 0; i < offSetList.size() - 1 - tmp.get(topic).get(partition).size(); i++) {
                        byte[] data = Utils.findData(offSetList, startingOffsetList, i, topic, partition);
                        if (data == null) {
                            break;
                        }
                        connection.send(Utils.preparePubReq(data, topic, (short) ((long) numPartitionsList.get(0))).array());
                        connection.waitForAck();
                    }
                    for (int i = 0; i < tmp.get(topic).get(partition).size(); i++) {
                        byte[] data = tmp.get(topic).get(partition).get(i);
                        connection.send(Utils.preparePubReq(data, topic, (short) ((long) numPartitionsList.get(0))).array());
                        connection.waitForAck();
                    }
                }
            }
            LOGGER.info("finished sending snapshot");
            connection.send(new byte[]{(byte) Constants.CAT_FIN});
            connection.waitForAck();
            connection.close();
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOGGER.error("sendSnapshot(): " + e.getMessage());
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
        Map<String, Map<Integer, List<List<Long>>>> topics = new HashMap<>(topicStruct.getTopics());
        if (topics.containsKey(topic) && topics.get(topic).containsKey(pullReq.getPartition())) {
            List<List<Long>> list = topics.get(topic).get(pullReq.getPartition());
            if (list.size() == 3) {
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
                    while (index < offSetList.size() - 1 && count < numMessages) {
                        byte[] data = Utils.findData(offSetList, startingOffsetList, index, pullReq.getTopic(), pullReq.getPartition());
                        if (data == null) {
                            break;
                        }
                        try {
                            connection.send(Utils.prepareData(Constants.REQ_RES, offSetList.get(index), data).array());
                        } catch (IOException | InterruptedException | ExecutionException e) {
                            LOGGER.error("processPullReq(): " + e.getMessage());
                        }
                        LOGGER.info("data at offset: " + offSetList.get(index) + " from topic: " + topic + ", partition: " + pullReq.getPartition() + " sent");
                        index++;
                        count++;
                    }
                }
            }
        }
    }

    /**
     * Method to send member information to requesting host.
     *
     * @param connection connection
     */
    private void processMemReq(Connection connection) {
        try {
            List<Membership.Broker> brokers = new ArrayList<>();
            LOGGER.info("sending members info");
            if (members == null) {
                Membership.MemberTable memberTable = Membership.MemberTable.newBuilder()
                        .setSize(-1)
                        .addAllBrokers(brokers).build();
                connection.send(memberTable.toByteArray());
                return;
            }
            members.sendMembers(connection);
        } catch (IOException | ExecutionException | InterruptedException e) {
            LOGGER.error("sendMembers(): " + e.getMessage());
        }
    }

    /**
     * Method to process replication request.
     *
     * @param connection connection
     * @param request    request
     */
    private void processRepReq(Connection connection, byte[] request) {
        try {
            repQueue.add(request);
            if (status.size() == 2) {
                status.remove(0);
            }
            status.add(request);
            LOGGER.info("sending ack to replicate request");
            connection.send(new byte[]{(byte) Constants.ACK_RES});
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOGGER.error("processRepReq(): " + e.getMessage());
        }
    }

    /**
     * Method to process message signaling end of catching up.
     *
     * @param connection connection
     */
    private void processCatFin(Connection connection) {
        try {
            LOGGER.info("done catching up");
            isCatchingUp = false;
            connection.send(new byte[]{(byte) Constants.ACK_RES});
        } catch (IOException | ExecutionException | InterruptedException e) {
            LOGGER.error("processCatFin: " + e.getMessage());
        }
    }

    /**
     * Method to send acknowledgement to election request.
     *
     * @param connection connection
     */
    private void processElectReq(Connection connection) {
        try {
            LOGGER.info("membership: sending acknowledgement for election request.");
            connection.send(new byte[]{(byte) Constants.ACK_RES});
        } catch (IOException | ExecutionException | InterruptedException e) {
            LOGGER.error(e.getMessage());
        }
        if (!members.isInElection()) {
            members.startElection();
        }
    }

    /**
     * Method to process victory message by updating the new leader in the members table.
     *
     * @param request victory message
     */
    private void processVicMess(byte[] request) {
        byte[] brokerBytes = new byte[request.length - 1];
        System.arraycopy(request, 1, brokerBytes, 0, brokerBytes.length);
        try {
            Membership.Broker broker = Membership.Broker.parseFrom(brokerBytes);
            BrokerMetadata leader = new BrokerMetadata(broker.getAddress(), broker.getPort(), broker.getPartition(), broker.getId(), broker.getIsAsync());
            LOGGER.info("membership: new leader: " + broker.getId());
            if (leader.getListenAddress().equals(host) && leader.getListenPort() == port) {
                isLeader = true;
                reconcileLog();
                while (!repQueue.isEmpty()) {
                    try {
                        repQueue.wait();
                    } catch (InterruptedException e) {
                        LOGGER.error(e.getMessage());
                    }
                }
                timer.cancel();
                timer = new Timer();
                brokerRegister = new BrokerRegister(curatorFramework,
                        new InstanceSerializerFactory(objectMapper.reader(), objectMapper.writer()),
                        Constants.SERVICE_NAME, host, port, leader.getPartition(), leader.getId(), leader.isAsync());
                brokerRegister.registerAvailability();
                LOGGER.info("membership: registering with Zookeeper");
            } else {
                members.setLeader(leader);
            }
        } catch (InvalidProtocolBufferException e) {
            LOGGER.error(e.getMessage());
        }
    }

    /**
     * Method to reconcile logs across different followers.
     */
    private void reconcileLog() {
        TreeMap<BrokerMetadata, Connection> followerList = new TreeMap<>(members.getFollowers());
        for (BrokerMetadata follower : followerList.keySet()) {
            try {
                AsynchronousSocketChannel socket = AsynchronousSocketChannel.open();
                Future<Void> future = socket.connect(new InetSocketAddress(follower.getListenAddress(), follower.getListenPort()));
                future.get();
                Connection followerConnection = new Connection(socket);
                if (follower.isAsync()) {
                    asyncFollowers.put(follower, followerConnection);
                    LOGGER.info("added to async follower list: " + follower.getId());
                } else {
                    syncFollowers.put(follower, followerConnection);
                    LOGGER.info("added to sync follower list: " + follower.getId());
                }
                LOGGER.info("membership: sending request to reconcile logs to: " + follower.getId());
                followerConnection.send(new byte[]{(byte) Constants.REC_REQ});
                byte[] message = followerConnection.receive();
                int count = 0;
                while (message == null && count < Constants.RETRY) {
                    message = followerConnection.receive();
                    count++;
                }
                if (message != null) {
                    Membership.Status statusPro = Membership.Status.parseFrom(message);
                    byte[] req0 = statusPro.getList(0).toByteArray();
                    byte[] req1 = statusPro.getList(1).toByteArray();
                    if (status.contains(req0) && !status.contains(req1)) {
                        LOGGER.info("membership: updating log");
                        repQueue.add(req1);
                        status.remove(0);
                        status.add(req1);
                    } else if (!status.contains(req0) && status.contains(req1)) {
                        LOGGER.info("membership: sending replication request");
                        followerConnection.send(status.get(1));
                        followerConnection.waitForAck();
                    }
                    LOGGER.info("membership: logs are same");
                }
            } catch (IOException | InterruptedException | ExecutionException e) {
                LOGGER.error(e.getMessage());
            }
        }
    }

    /**
     * Method to send the status list over the connection.
     *
     * @param connection connection
     */
    private void processRecReq(Connection connection) {
        try {
            LOGGER.info("membership: sending log status");
            List<ByteString> statusList = new ArrayList<>();
            for (byte[] bytes : status) {
                statusList.add(ByteString.copyFrom(bytes));
            }
            Membership.Status statusProto = Membership.Status.newBuilder()
                    .addAllList(statusList)
                    .build();
            connection.send(statusProto.toByteArray());
            members.setInElection(false);
        } catch (IOException | ExecutionException | InterruptedException e) {
            LOGGER.error(e.getMessage());
        }
    }

    /**
     * Method to close the broker.
     */
    public void close() {
        try {
            LOGGER.info("closing broker");
            isRunning = false;
            timer.cancel();
            for (Connection connection : asyncFollowers.values()) {
                connection.close();
            }
            for (Connection connection : syncFollowers.values()) {
                connection.close();
            }
            if (brokerRegister != null) {
                brokerRegister.unregisterAvailability();
            }
            members.close();
            server.close();
        } catch (IOException e) {
            LOGGER.error("close(): " + e.getMessage());
        }
    }

}

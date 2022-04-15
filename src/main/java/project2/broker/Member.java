package project2.broker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import project2.Config;
import project2.Connection;
import project2.Constants;
import project2.broker.protos.Membership;
import project2.zookeeper.BrokerMetadata;
import project2.zookeeper.BrokerRegister;
import project2.zookeeper.InstanceSerializerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Class that manages the broker membership by sending heartbeat messages to exchange membership and handle failure
 * when detected.
 *
 * @author anhnguyen
 */
public class Member {
    /**
     * Logger.
     */
    private final Logger LOGGER = LoggerFactory.getLogger("membership");
    /**
     * leader.
     */
    private BrokerMetadata leader;
    /**
     * followers.
     */
    private final TreeMap<BrokerMetadata, Connection> followers;
    /**
     * timer.
     */
    private final Timer timer;
    /**
     * in election status.
     */
    private volatile boolean inElection;
    /**
     * current broker host.
     */
    private final String host;
    /**
     * current broker port.
     */
    private final int port;
    /**
     * current broker id.
     */
    private final int id;
    /**
     * current broker partition.
     */
    private final int partition;
    /**
     * id removal list.
     */
    private final List<Integer> removal;
    /**
     * counter for response during election.
     */
    private int numResp;
    /**
     * curator framework.
     */
    private final CuratorFramework curatorFramework;
    /**
     * object mapper.
     */
    private final ObjectMapper objectMapper;

    /**
     * Constructor.
     *
     * @param config config
     */
    public Member(Config config, CuratorFramework curatorFramework, ObjectMapper objectMapper) {
        this.host = config.getHost();
        this.port = config.getPort();
        this.partition = config.getPartition();
        this.id = config.getId();
        this.numResp = 0;
        this.curatorFramework = curatorFramework;
        this.objectMapper = objectMapper;
        if (config.isLeader()) {
            this.leader = new BrokerMetadata(this.host, this.port, config.getPartition(), config.getId());
            LOGGER.info("leader: " + config.getId());
        }
        this.followers = new TreeMap<>(Comparator.comparingInt(BrokerMetadata::getId));
        BrokerMetadata follower = new Gson().fromJson(config.getMembers(), BrokerMetadata.class);
        connectWithFollower(follower);
        this.inElection = false;
        this.removal = new ArrayList<>();
        this.timer = new Timer();
        this.timer.schedule(new TimerTask() {
            @Override
            public void run() {
                exchangeInfo();
            }
        }, 0, Constants.INTERVAL);
    }

    /**
     * Method to try connecting with follower.
     *
     * @param follower follower
     */
    private void connectWithFollower(BrokerMetadata follower) {
        AsynchronousSocketChannel socket = null;
        try {
            boolean connected = false;
            while (!connected) {
                try {
                    synchronized (this) {
                        wait(Constants.TIME_OUT);
                    }
                    LOGGER.info("trying to connect with follower: " + follower.getId());
                    socket = AsynchronousSocketChannel.open();
                    Future<Void> future = socket.connect(new InetSocketAddress(follower.getListenAddress(), follower.getListenPort()));
                    future.get();
                    connected = true;
                } catch (ExecutionException | InterruptedException e) {
                    if (socket != null) {
                        socket.close();
                    }
                }
            }
            LOGGER.info("connected with follower: " + follower.getId());
            Connection connection = new Connection(socket);
            followers.put(follower, connection);
            LOGGER.info("added follower to list");
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
        }
    }

    /**
     * Method to iterate the followers list and request membership information from that follower.
     */
    private void exchangeInfo() {
        if (!inElection) {
            TreeMap<BrokerMetadata, Connection> copy = new TreeMap<>(Comparator.comparingInt(BrokerMetadata::getId));
            for (BrokerMetadata broker : followers.keySet()) {
                copy.put(broker, followers.get(broker));
            }
            for (BrokerMetadata broker : copy.keySet()) {
                try {
                    LOGGER.info("requesting membership info from: " + broker.getId());
                    Connection connection = copy.get(broker);
                    connection.send(new byte[]{(byte) Constants.MEM_REQ});
                    byte[] resp = connection.receive();
                    int count = 0;
                    while (resp == null && count < Constants.RETRY) {
                        resp = connection.receive();
                        count++;
                    }
                    if (resp == null) {
                        handleFailure(broker);
                    } else {
                        updateMembers(resp);
                    }
                } catch (IOException | InterruptedException | ExecutionException e) {
                    LOGGER.error("exchangeInfo(): " + e.getMessage());
                    handleFailure(broker);
                }
            }
        }
    }

    /**
     * Getter for leader.
     *
     * @return leader
     */
    public synchronized BrokerMetadata getLeader() {
        return leader;
    }

    /**
     * Getter for follower.
     *
     * @return follower
     */
    public synchronized TreeMap<BrokerMetadata, Connection> getFollowers() {
        return followers;
    }

    /**
     * Setter for leader.
     *
     * @param leader leader
     */
    public synchronized void setLeader(BrokerMetadata leader) {
        this.leader = leader;
    }

    /**
     * Getter for inElection status.
     *
     * @return in election
     */
    public boolean isInElection() {
        return inElection;
    }

    /**
     * Setter for inElection status.
     *
     * @param inElection status
     */
    public void setInElection(boolean inElection) {
        this.inElection = inElection;
    }

    /**
     * Method to handle failure.
     *
     * @param broker failed broker
     */
    private synchronized void handleFailure(BrokerMetadata broker) {
        followers.remove(broker);
        removal.add(broker.getId());
        LOGGER.info("removed failed broker: " + broker.getId());
        if (leader != null && broker.getId() == leader.getId()) {
            startElection();
        }
    }

    /**
     * Method to update the membership based on information collected from other brokers.
     *
     * @param resp response for membership request
     */
    private synchronized void updateMembers(byte[] resp) {
        try {
            Membership.MemberTable memberTable = Membership.MemberTable.parseFrom(resp);
            if (memberTable.getSize() == -1) {
                return;
            }
            for (int i = 0; i < memberTable.getBrokersCount(); i++) {
                Membership.Broker broker = memberTable.getBrokers(i);
                BrokerMetadata brokerMetadata = new BrokerMetadata(broker.getAddress(), broker.getPort(), broker.getPartition(), broker.getId());
                if (i == 0 && !broker.getAddress().equals(Constants.NONE) && leader == null && !removal.contains(brokerMetadata.getId())) {
                    leader = brokerMetadata;
                    LOGGER.info("leader: " + leader.getId());
                }
                if (!followers.containsKey(brokerMetadata) && !broker.getAddress().equals(Constants.NONE)
                        && (!brokerMetadata.getListenAddress().equals(host) || brokerMetadata.getListenPort() != port)
                        && !removal.contains(brokerMetadata.getId())) {
                    AsynchronousSocketChannel socket = AsynchronousSocketChannel.open();
                    Future<Void> future = socket.connect(new InetSocketAddress(brokerMetadata.getListenAddress(), brokerMetadata.getListenPort()));
                    future.get();
                    LOGGER.info("connected with follower: " + brokerMetadata.getId());
                    Connection followerConnection = new Connection(socket);
                    followers.put(brokerMetadata, followerConnection);
                    LOGGER.info("added follower to list");
                }
            }
        } catch (IOException | ExecutionException | InterruptedException e) {
            LOGGER.error(e.getMessage());
        }
    }

    /**
     * Method to elect a new leader.
     */
    public synchronized void startElection() {
        inElection = true;
        BrokerMetadata failedBroker = null;
        if (leader != null) {
            failedBroker = new BrokerMetadata(leader.getListenAddress(), leader.getListenPort(), leader.getPartition(), leader.getId());
        }
        leader = null;
        LOGGER.info("starting election");
        for (BrokerMetadata broker : followers.keySet()) {
            if (broker.getId() > id) {
                break;
            }
            if (!removal.contains(broker.getId())) {
                Thread t = new Thread(() -> sendElectReq(broker));
                t.start();
            }
        }

        if (numResp == 0) {
            try {
                LOGGER.info("Waiting for response from lower id followers");
                wait(Constants.TIME_OUT);
            } catch (InterruptedException e) {
                LOGGER.error(e.getMessage());
            }
        }

        if (numResp == 0) {
            LOGGER.info("No response from lower id followers. Electing self as leader");
            if (failedBroker != null) {
                BrokerRegister brokerRegister = new BrokerRegister(curatorFramework,
                        new InstanceSerializerFactory(objectMapper.reader(), objectMapper.writer()),
                        Constants.SERVICE_NAME, failedBroker.getListenAddress(), failedBroker.getListenPort(), failedBroker.getPartition(), failedBroker.getId());
                LOGGER.info("Deregister failed broker with Zookeeper");
                brokerRegister.unregisterAvailability();
            }
            leader = new BrokerMetadata(host, port, partition, id);
            byte[] vicMess = prepareVicMess();
            try {
                AsynchronousSocketChannel socket = AsynchronousSocketChannel.open();
                Future<Void> future = socket.connect(new InetSocketAddress(host, port));
                future.get();
                Connection connection = new Connection(socket);
                connection.send(vicMess);
                connection.close();

                for (BrokerMetadata broker : followers.keySet()) {
                    if (!removal.contains(broker.getId())) {
                        LOGGER.info("sending victory message to: " + broker.getId());
                        followers.get(broker).send(vicMess);
                    }
                }
            } catch (IOException | InterruptedException | ExecutionException e) {
                LOGGER.error(e.getMessage());
            }
        } else {
            if (leader == null) {
                try {
                    LOGGER.info("got response from lower id follower. wait for leader to be announced.");
                    wait(Constants.TIME_OUT);
                } catch (InterruptedException e) {
                    LOGGER.error(e.getMessage());
                }
            }
            if (leader == null) {
                LOGGER.info("timeout. no leader announced. starting election again");
                numResp = 0;
                startElection();
            } else {
                LOGGER.info("leader selected: " + leader.getId());
            }
        }
        inElection = false;
        numResp = 0;
        LOGGER.info("end of election");
    }

    /**
     * Method to wrap the new leader info around byte array to be sent in the victory message.
     *
     * @return byte array
     */
    private byte[] prepareVicMess() {
        Membership.Broker newLeader = Membership.Broker.newBuilder()
                .setAddress(host)
                .setPort(port)
                .setPartition(partition)
                .setId(id)
                .build();
        ByteBuffer byteBuffer = ByteBuffer.allocate(newLeader.toByteArray().length + 1);
        byteBuffer.put((byte) Constants.VIC_MESS);
        byteBuffer.put(newLeader.toByteArray());
        return byteBuffer.array();
    }

    /**
     * Method to send an election request.
     *
     * @param broker broker to send election request to
     */
    private void sendElectReq(BrokerMetadata broker) {
        try {
            LOGGER.info("sending election request to: " + broker.getId());
            Connection connection = followers.get(broker);
            connection.send(new byte[]{(byte) Constants.ELECT_REQ});
            byte[] resp = connection.receive();
            int count = 0;
            while (resp == null && count < Constants.RETRY) {
                resp = connection.receive();
                count++;
            }
            if (resp != null) {
                numResp++;
            }
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOGGER.error(e.getMessage());
        }
    }

    /**
     * Method to close the connection and cancel timer in membership.
     */
    public void close() {
        timer.cancel();
        for (Connection connection : followers.values()) {
            connection.close();
        }
    }

    /**
     * Method to send member information to requesting host.
     *
     * @param connection connection
     */
    public void sendMembers(Connection connection) {
        try {
            List<Membership.Broker> brokers = new ArrayList<>();
            LOGGER.info("sending members info");
            BrokerMetadata leader = getLeader();
            Map<BrokerMetadata, Connection> followers = getFollowers();
            Membership.Broker leaderBroker;
            if (leader == null) {
                leaderBroker = getBrokerInfo(null);
            } else {
                leaderBroker = getBrokerInfo(leader);
            }
            brokers.add(leaderBroker);
            for (BrokerMetadata follower : followers.keySet()) {
                Membership.Broker followingBroker = getBrokerInfo(follower);
                brokers.add(followingBroker);
            }
            Membership.MemberTable memberTable = Membership.MemberTable.newBuilder()
                    .setSize(followers.size() + 1)
                    .addAllBrokers(brokers)
                    .build();
            connection.send(memberTable.toByteArray());
        } catch (IOException | ExecutionException | InterruptedException e) {
            LOGGER.error("sendMembers(): " + e.getMessage());
        }
    }

    /**
     * Method to wrap the broker metadata around the Broker protobuf.
     *
     * @param broker broker metadata
     * @return Broker protobuf
     */
    private Membership.Broker getBrokerInfo(BrokerMetadata broker) {
        Membership.Broker brokerProto;
        if (broker == null) {
            brokerProto = Membership.Broker.newBuilder()
                    .setAddress(Constants.NONE)
                    .setPort(0).setPartition(0)
                    .setPartition(0)
                    .build();
        } else {
            brokerProto = Membership.Broker.newBuilder()
                    .setAddress(broker.getListenAddress())
                    .setPort(broker.getListenPort())
                    .setPartition(broker.getPartition())
                    .setId(broker.getId())
                    .build();
        }
        return brokerProto;
    }
}

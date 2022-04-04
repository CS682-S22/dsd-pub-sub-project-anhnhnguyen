package project2.broker;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import project2.Config;
import project2.Connection;
import project2.Constants;
import project2.zookeeper.BrokerMetadata;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.TreeMap;
import java.util.concurrent.*;

public class Member {
    private final Logger LOGGER = LoggerFactory.getLogger(Member.class);
    private BrokerMetadata leader;
    private final TreeMap<BrokerMetadata, Connection> followers;
    private final ScheduledExecutorService scheduler;
    private volatile boolean inElection;
    private final String host;
    private final int port;

    public Member(Config config) {
        this.host = config.getHost();
        this.port = config.getPort();
        if (config.isLeader()) {
            leader = new BrokerMetadata(host, port, config.getPartition(), config.getId());
            LOGGER.info("leader: " + config.getId());
        }
        this.followers = new TreeMap<>((o1, o2) -> Integer.compare(o2.getId(), o1.getId()));
        BrokerMetadata follower = new Gson().fromJson(config.getMembers(), BrokerMetadata.class);
        try {
            AsynchronousSocketChannel socket = AsynchronousSocketChannel.open();
            boolean connected = false;
            while (!connected) {
                try {
                    synchronized (this) {
                        wait(Constants.TIME_OUT);
                    }
                    Future<Void> future = socket.connect(new InetSocketAddress(follower.getListenAddress(), follower.getListenPort()));
                    future.get();
                    connected = true;
                } catch (ExecutionException | InterruptedException e) {
                    // do nothing, wait till the other host connects
                }
            }
            LOGGER.info("connected with follower: " + follower.getId());
            Connection connection = new Connection(socket);
            followers.put(follower, connection);
            LOGGER.info("added follower to list");
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
        }
        this.inElection = false;
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.scheduler.scheduleWithFixedDelay(this::exchangeInfo, 0, Constants.INTERVAL, TimeUnit.MILLISECONDS);
    }

    private void exchangeInfo() {
        if (!inElection) {
            LOGGER.info("requesting membership info");
            for (BrokerMetadata broker : followers.keySet()) {
                try {
                    Connection connection = followers.get(broker);
                    connection.send(Constants.MEM.getBytes(StandardCharsets.UTF_8));
                    byte[] resp = connection.receive();
                    int count = 0;
                    while (resp == null && count < Constants.RETRY) {
                        resp = connection.receive();
                        count++;
                    }
                    if (resp == null) {
                        handleFailure();
                    } else {
                        updateMembers(resp, connection);
                    }
                } catch (IOException | InterruptedException | ExecutionException e) {
                    handleFailure();
                }
            }
        }
    }

    public BrokerMetadata getLeader() {
        return leader;
    }

    public TreeMap<BrokerMetadata, Connection> getFollowers() {
        return followers;
    }

    private void handleFailure() {

    }

    private void updateMembers(byte[] resp, Connection connection) {
        int numBrokers = new BigInteger(resp).intValue();
        int count = 0;
        int retries = 0;
        synchronized (this) {
            while (count < numBrokers && retries < Constants.RETRY) {
                try {
                    byte[] brokerInfo = connection.receive();
                    if (brokerInfo != null) {
                        BrokerDecoder broker = new BrokerDecoder(brokerInfo);
                        BrokerMetadata brokerMetadata = new BrokerMetadata(broker.getHost(), broker.getPort(), broker.getPartition(), broker.getId());
                        if (count == 0 && !broker.getHost().equals(Constants.NONE) && leader == null) {
                            leader = brokerMetadata;
                            LOGGER.info("leader: " + leader.getId());
                        }
                        if (!followers.containsKey(brokerMetadata) && !broker.getHost().equals(Constants.NONE) && (!brokerMetadata.getListenAddress().equals(host) || brokerMetadata.getListenPort() != port)) {
                            AsynchronousSocketChannel socket = AsynchronousSocketChannel.open();
                            Future<Void> future = socket.connect(new InetSocketAddress(brokerMetadata.getListenAddress(), brokerMetadata.getListenPort()));
                            future.get();
                            LOGGER.info("connected with follower: " + brokerMetadata.getId());
                            Connection followerConnection = new Connection(socket);
                            followers.put(brokerMetadata, followerConnection);
                            LOGGER.info("added follower to list");
                        }
                        count++;
                        retries = 0;
                    } else {
                        retries++;
                    }
                } catch (IOException | InterruptedException | ExecutionException e) {
                    handleFailure();
                }
            }
            if (count != numBrokers && retries == Constants.RETRY) {
                handleFailure();
            }
        }
    }

    private void startElection() {

    }

    public void close() {
        try {
            scheduler.shutdownNow();
            if (!scheduler.awaitTermination(Constants.TIME_OUT, TimeUnit.MILLISECONDS)) {
                LOGGER.error("awaitTermination()");
            }
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage());
        }
    }
}

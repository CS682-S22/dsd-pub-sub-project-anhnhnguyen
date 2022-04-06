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
import java.util.Comparator;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Member {
    private final Logger LOGGER = LoggerFactory.getLogger(Member.class);
    private BrokerMetadata leader;
    private final TreeMap<BrokerMetadata, Connection> followers;
    private final Timer timer;
    private volatile boolean inElection;
    private final String host;
    private final int port;

    public Member(Config config) {
        this.host = config.getHost();
        this.port = config.getPort();
        if (config.isLeader()) {
            this.leader = new BrokerMetadata(this.host, this.port, config.getPartition(), config.getId());
            LOGGER.info("leader: " + config.getId());
        }
        this.followers = new TreeMap<>(Comparator.comparingInt(BrokerMetadata::getId));
        BrokerMetadata follower = new Gson().fromJson(config.getMembers(), BrokerMetadata.class);
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
            this.followers.put(follower, connection);
            LOGGER.info("added follower to list");
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
        }
        this.inElection = false;
        this.timer = new Timer();
        this.timer.schedule(new TimerTask() {
            @Override
            public void run() {
                exchangeInfo();
            }
        }, 0, Constants.INTERVAL);
    }

    private void exchangeInfo() {
        if (!inElection) {
            LOGGER.info("requesting membership info");
            TreeMap<BrokerMetadata, Connection> copy = new TreeMap<>(Comparator.comparingInt(BrokerMetadata::getId));
            for (BrokerMetadata broker : followers.keySet()) {
                copy.put(broker, followers.get(broker));
            }
            for (BrokerMetadata broker : copy.keySet()) {
                try {
                    Connection connection = copy.get(broker);
                    byte[] req = new byte[1];
                    req[0] = Constants.MEM_REQ;
                    connection.send(req);
                    byte[] resp = connection.receive();
                    int count = 0;
                    while (resp == null && count < Constants.RETRY) {
                        resp = connection.receive();
                        count++;
                    }
                    if (resp == null) {
                        handleFailure(broker);
                    } else {
                        updateMembers(resp, connection, broker);
                    }
                } catch (IOException | InterruptedException | ExecutionException e) {
                    LOGGER.error("exchangeInfo(): " + e.getMessage());
                    handleFailure(broker);
                }
            }
        }
    }

    public synchronized BrokerMetadata getLeader() {
        return leader;
    }

    public synchronized TreeMap<BrokerMetadata, Connection> getFollowers() {
        return followers;
    }

    private void handleFailure(BrokerMetadata broker) {
        if (broker.equals(leader)) {
            startElection();
        }
        followers.remove(broker);
        LOGGER.info("removed failed broker");
    }

    private synchronized void updateMembers(byte[] resp, Connection connection, BrokerMetadata bm) {
        int numBrokers = new BigInteger(resp).intValue();
        int count = 0;
        int retries = 0;
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
                LOGGER.error("updateMembers(): " + e.getMessage());
                handleFailure(bm);
            }
        }
        if (count != numBrokers && retries == Constants.RETRY) {
            handleFailure(bm);
        }
    }

    private void startElection() {

    }

    public void close() {
        timer.cancel();
        for (Connection connection : followers.values()) {
            connection.close();
        }
    }
}

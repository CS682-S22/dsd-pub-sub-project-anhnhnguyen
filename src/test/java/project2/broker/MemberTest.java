package project2.broker;

import com.google.gson.Gson;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import project2.Config;
import project2.Connection;
import project2.zookeeper.BrokerMetadata;
import project2.zookeeper.Curator;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class MemberTest {

    private Config config1;
    private Config config1a;
    private Member member1;
    private Member member1a;
    private Member member1b;
    private Member member1c;
    private Member member1d;
    private Broker broker1;
    private Broker broker1a;
    private Broker broker1b;
    private Broker broker1c;
    private Broker broker1d;

    @BeforeEach
    void setUp() {
        try {
            Curator curator = new Curator("127.0.0.1:2181");

            config1 = new Gson().fromJson(new FileReader("configs/broker1.json"), Config.class);
            config1a = new Gson().fromJson(new FileReader("configs/broker1a.json"), Config.class);
            Config config1b = new Gson().fromJson(new FileReader("configs/broker1b.json"), Config.class);
            Config config1c = new Gson().fromJson(new FileReader("configs/broker1c.json"), Config.class);
            Config config1d = new Gson().fromJson(new FileReader("configs/broker1d.json"), Config.class);

            Thread t1 = new Thread(() -> {
                broker1 = new Broker(config1, curator.getCuratorFramework(), curator.getObjectMapper());
                broker1.start();
            });

            Thread t1a = new Thread(() -> {
                broker1a = new Broker(config1a, curator.getCuratorFramework(), curator.getObjectMapper());
                broker1a.start();
            });

            Thread t1b = new Thread(() -> {
                broker1b = new Broker(config1b, curator.getCuratorFramework(), curator.getObjectMapper());
                broker1b.start();
            });

            Thread t1c = new Thread(() -> {
                broker1c = new Broker(config1c, curator.getCuratorFramework(), curator.getObjectMapper());
                broker1c.start();
            });

            Thread t1d = new Thread(() -> {
                broker1d = new Broker(config1d, curator.getCuratorFramework(), curator.getObjectMapper());
                broker1d.start();
            });

            t1.start();
            Thread.sleep(1000);
            t1a.start();
            Thread.sleep(1000);
            t1b.start();
            Thread.sleep(1000);
            t1c.start();
            Thread.sleep(1000);
            t1d.start();

            member1 = new Member(config1, curator.getCuratorFramework(), curator.getObjectMapper());
            member1a = new Member(config1a, curator.getCuratorFramework(), curator.getObjectMapper());
            member1b = new Member(config1b, curator.getCuratorFramework(), curator.getObjectMapper());
            member1c = new Member(config1c, curator.getCuratorFramework(), curator.getObjectMapper());
            member1d = new Member(config1d, curator.getCuratorFramework(), curator.getObjectMapper());

        } catch (FileNotFoundException | InterruptedException e) {
            fail(e.getMessage());
        }
    }

    @AfterEach
    void tearDown() {
        broker1.close();
        broker1a.close();
        broker1b.close();
        broker1c.close();
        broker1d.close();
        member1.close();
        member1a.close();
        member1b.close();
        member1c.close();
        member1d.close();
    }

    @Test
    void testGetLeaderNoFailure() {
        BrokerMetadata leader = member1.getLeader();
        assertEquals(leader.getListenAddress(), config1.getHost());
        assertEquals(leader.getListenPort(), config1.getPort());
        assertEquals(leader.getPartition(), config1.getPartition());
        assertEquals(leader.getId(), config1.getId());

        leader = member1a.getLeader();
        assertEquals(leader.getListenAddress(), config1.getHost());
        assertEquals(leader.getListenPort(), config1.getPort());
        assertEquals(leader.getPartition(), config1.getPartition());
        assertEquals(leader.getId(), config1.getId());

        leader = member1b.getLeader();
        assertEquals(leader.getListenAddress(), config1.getHost());
        assertEquals(leader.getListenPort(), config1.getPort());
        assertEquals(leader.getPartition(), config1.getPartition());
        assertEquals(leader.getId(), config1.getId());

        leader = member1c.getLeader();
        assertEquals(leader.getListenAddress(), config1.getHost());
        assertEquals(leader.getListenPort(), config1.getPort());
        assertEquals(leader.getPartition(), config1.getPartition());
        assertEquals(leader.getId(), config1.getId());

        leader = member1d.getLeader();
        assertEquals(leader.getListenAddress(), config1.getHost());
        assertEquals(leader.getListenPort(), config1.getPort());
        assertEquals(leader.getPartition(), config1.getPartition());
        assertEquals(leader.getId(), config1.getId());
    }

    @Test
    void testGetFollowersNoFailure() {
        TreeMap<BrokerMetadata, Connection> followers = member1.getFollowers();
        assertEquals(followers.keySet().size(), 4);

        followers = member1a.getFollowers();
        assertEquals(followers.keySet().size(), 4);

        followers = member1b.getFollowers();
        assertEquals(followers.keySet().size(), 4);

        followers = member1c.getFollowers();
        assertEquals(followers.keySet().size(), 4);

        followers = member1d.getFollowers();
        assertEquals(followers.keySet().size(), 4);
    }

    @Test
    void testGetLeaderFollowerFailure() {
        broker1d.close();
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            fail(e.getMessage());
        }
        BrokerMetadata leader = member1.getLeader();
        assertEquals(leader.getListenAddress(), config1.getHost());
        assertEquals(leader.getListenPort(), config1.getPort());
        assertEquals(leader.getPartition(), config1.getPartition());
        assertEquals(leader.getId(), config1.getId());

        leader = member1a.getLeader();
        assertEquals(leader.getListenAddress(), config1.getHost());
        assertEquals(leader.getListenPort(), config1.getPort());
        assertEquals(leader.getPartition(), config1.getPartition());
        assertEquals(leader.getId(), config1.getId());

        leader = member1b.getLeader();
        assertEquals(leader.getListenAddress(), config1.getHost());
        assertEquals(leader.getListenPort(), config1.getPort());
        assertEquals(leader.getPartition(), config1.getPartition());
        assertEquals(leader.getId(), config1.getId());

        leader = member1c.getLeader();
        assertEquals(leader.getListenAddress(), config1.getHost());
        assertEquals(leader.getListenPort(), config1.getPort());
        assertEquals(leader.getPartition(), config1.getPartition());
        assertEquals(leader.getId(), config1.getId());
    }

    @Test
    void testGetFollowersFollowerFailure() {
        broker1d.close();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            fail(e.getMessage());
        }
        TreeMap<BrokerMetadata, Connection> followers = member1.getFollowers();
        assertEquals(followers.keySet().size(), 3);

        followers = member1a.getFollowers();
        assertEquals(followers.keySet().size(), 3);

        followers = member1b.getFollowers();
        assertEquals(followers.keySet().size(), 3);

        followers = member1c.getFollowers();
        assertEquals(followers.keySet().size(), 3);
    }

    @Test
    void testGetLeaderLeaderFailure() {
        broker1.close();
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            fail(e.getMessage());
        }
        BrokerMetadata leader = member1a.getLeader();
        assertEquals(leader.getListenAddress(), config1a.getHost());
        assertEquals(leader.getListenPort(), config1a.getPort());
        assertEquals(leader.getPartition(), config1a.getPartition());
        assertEquals(leader.getId(), config1a.getId());
    }

    @Test
    void testGetFollowersLeaderFailure() {
        broker1.close();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            fail(e.getMessage());
        }
        TreeMap<BrokerMetadata, Connection> followers = member1a.getFollowers();
        assertEquals(followers.keySet().size(), 3);

        followers = member1b.getFollowers();
        assertEquals(followers.keySet().size(), 3);

        followers = member1c.getFollowers();
        assertEquals(followers.keySet().size(), 3);

        followers = member1d.getFollowers();
        assertEquals(followers.keySet().size(), 3);
    }
}
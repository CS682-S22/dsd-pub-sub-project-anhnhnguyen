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
    private Member member1;
    private Member member1a;
    private Member member1b;
    private Member member1c;
    private Member member1d;

    @BeforeEach
    void setUp() {
        try {
            Curator curator = new Curator("127.0.0.1:2181");

            config1 = new Gson().fromJson(new FileReader("configs/broker1.json"), Config.class);
            Config config1a = new Gson().fromJson(new FileReader("configs/broker1a.json"), Config.class);
            Config config1b = new Gson().fromJson(new FileReader("configs/broker1b.json"), Config.class);
            Config config1c = new Gson().fromJson(new FileReader("configs/broker1c.json"), Config.class);
            Config config1d = new Gson().fromJson(new FileReader("configs/broker1d.json"), Config.class);

            Thread t1 = new Thread(() -> {
                Broker broker = new Broker(config1, curator.getCuratorFramework(), curator.getObjectMapper());
                broker.start();
            });

            Thread t1a = new Thread(() -> {
                Broker broker = new Broker(config1a, curator.getCuratorFramework(), curator.getObjectMapper());
                broker.start();
            });

            Thread t1b = new Thread(() -> {
                Broker broker = new Broker(config1b, curator.getCuratorFramework(), curator.getObjectMapper());
                broker.start();
            });

            Thread t1c = new Thread(() -> {
                Broker broker = new Broker(config1c, curator.getCuratorFramework(), curator.getObjectMapper());
                broker.start();
            });

            Thread t1d = new Thread(() -> {
                Broker broker = new Broker(config1d, curator.getCuratorFramework(), curator.getObjectMapper());
                broker.start();
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

            member1 = new Member(config1);
            member1a = new Member(config1a);
            member1b = new Member(config1b);
            member1c = new Member(config1c);
            member1d = new Member(config1d);

        } catch (FileNotFoundException | InterruptedException e) {
            fail(e.getMessage());
        }
    }

    @AfterEach
    void tearDown() {
        member1.close();
        member1a.close();
        member1b.close();
        member1c.close();
        member1d.close();
    }

    @Test
    void testGetLeader() {
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
    void testGetFollowers() {
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
}
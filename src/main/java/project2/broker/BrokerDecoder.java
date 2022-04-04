package project2.broker;

import project2.Utils;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

public class BrokerDecoder {
    private final String host;
    private final int port;
    private final int partition;
    private final int id;

    public BrokerDecoder(byte[] brokerInfo) {
        byte[] portBytes = Utils.extractBytes(0, 2, brokerInfo, false);
        this.port = new BigInteger(portBytes).intValue();

        byte[] partitionBytes = Utils.extractBytes(2, 4, brokerInfo, false);
        this.partition = new BigInteger(partitionBytes).intValue();

        byte[] idBytes = Utils.extractBytes(4, 6, brokerInfo, false);
        this.id = new BigInteger(idBytes).intValue();

        byte[] hostBytes = Utils.extractBytes(6, brokerInfo.length, brokerInfo, false);
        this.host = new String(hostBytes, StandardCharsets.UTF_8);
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getPartition() {
        return partition;
    }

    public int getId() {
        return id;
    }
}

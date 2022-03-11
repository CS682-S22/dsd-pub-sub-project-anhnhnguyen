package project2.consumer;

import project2.Constants;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

public class PullReq {
    private final String topic;
    private final long startingPosition;

    public PullReq(byte[] message) {
        int i = 1;
        int j = 0;

        byte[] tmp = new byte[Constants.BYTE_ALLOCATION];
        while (i < message.length && message[i] != 0) {
            tmp[j] = message[i];
            i++;
            j++;
        }
        byte[] topicBytes = new byte[j];
        System.arraycopy(tmp, 0, topicBytes, 0, j);
        this.topic = new String(topicBytes, StandardCharsets.UTF_8);

        tmp = new byte[Constants.BYTE_ALLOCATION];
        i++;
        j = 0;
        while (i < message.length) {
            tmp[j] = message[i];
            i++;
            j++;
        }
        byte[] positionBytes = new byte[j];
        System.arraycopy(tmp, 0, positionBytes, 0, j);
        this.startingPosition = new BigInteger(positionBytes).longValue();
    }

    public String getTopic() {
        return topic;
    }

    public long getStartingPosition() {
        return startingPosition;
    }
}

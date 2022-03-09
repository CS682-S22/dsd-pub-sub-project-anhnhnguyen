package project2.broker;

import project2.Constants;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

public class ReqRes {
    private final long offset;
    private final String key;
    private final byte[] data;

    public ReqRes(byte[] message) {
        int i = 1;
        int j = 0;

        byte[] tmp = new byte[Constants.BYTE_ALLOCATION];
        while (i < 9) {
            tmp[j] = message[i];
            i++;
            j++;
        }
        byte[] offsetBytes = new byte[j];
        System.arraycopy(tmp, 0, offsetBytes, 0, j);
        this.offset = new BigInteger(offsetBytes).longValue();

        tmp = new byte[Constants.BYTE_ALLOCATION];
        j = 0;
        while (i < message.length && message[i] != 0) {
            tmp[j] = message[i];
            i++;
            j++;
        }
        byte[] keyBytes = new byte[j];
        System.arraycopy(tmp, 0, keyBytes, 0, j);
        this.key = new String(keyBytes, StandardCharsets.UTF_8);

        tmp = new byte[Constants.BYTE_ALLOCATION];
        i++;
        j = 0;
        while (i < message.length) {
            tmp[j] = message[i];
            i++;
            j++;
        }
        this.data = new byte[j];
        System.arraycopy(tmp, 0, data, 0, j);
    }

    public long getOffset() {
        return offset;
    }

    public String getKey() {
        return key;
    }

    public byte[] getData() {
        return data;
    }
}

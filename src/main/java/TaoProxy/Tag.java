package TaoProxy;

import java.util.Arrays;
import java.util.Objects;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.Longs;

/**
 * @brief Interface to represent a block
 */
public class Tag implements Comparable<Tag>{
    public long seqNum = 0;
    public short clientID = 0;

    public int compareTo(Tag other){
        if (seqNum < other.seqNum) return -1;
        if (seqNum > other.seqNum) return 1;

        if (clientID < other.clientID) return -1;
        if (clientID > other.clientID) return 1;

        return 0;
    }

    public byte[] serialize() {
        byte[] seqNumBytes = Longs.toByteArray(seqNum);
        byte[] clientIDBytes = Shorts.toByteArray(clientID);
        byte[] serialized = Bytes.concat(seqNumBytes, clientIDBytes);
        return serialized;
    };

    public void initFromSerialized(byte[] serialized) {
        seqNum = Longs.fromByteArray(Arrays.copyOfRange(serialized, 0, 8));
        clientID = Shorts.fromByteArray(Arrays.copyOfRange(serialized, 8, 10));
    };

    public String toString() {
        return seqNum + ":" + clientID;
    }
}

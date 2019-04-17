package TaoClient;

import java.util.Arrays;
import java.util.Objects;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.Longs;

public class OperationID {
    public long seqNum = 0;
    public short clientID = 0;

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

    public OperationID getNext() {
        OperationID nextOpID = new OperationID();
        nextOpID.seqNum = seqNum + 1;
        nextOpID.clientID = clientID;
        return nextOpID;
    }

    public String toString() {
        return seqNum + ":" + clientID;
    }

    @Override
    public int hashCode() {
        return (int)(seqNum+clientID);
    }

    //Compare only account numbers
    @Override
    public boolean equals(Object rhs) {
        if (this == rhs)
            return true;
        if (rhs == null)
            return false;
        if (getClass() != rhs.getClass())
            return false;
        OperationID other = (OperationID) rhs;
        return (seqNum == other.seqNum && clientID == other.clientID);
    }
}

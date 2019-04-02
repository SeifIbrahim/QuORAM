package TaoProxy;

import Configuration.TaoConfigs;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.Longs;

import java.util.Arrays;
import java.util.Objects;

/**
 * Implementation of a block for TaoStore implementing the Block interface
 */
public class TaoBlock implements Block {
    // The ID of this block
    private long mID;

    // The data of this block
    private byte[] mData;

    private Tag mTag;

    /**
     * @brief Default constructor
     */
    public TaoBlock() {
        mID = -1;
        mData = new byte[TaoConfigs.BLOCK_SIZE];
        mTag = new Tag();
    }

    /**
     * @brief Constructor that takes in a block ID
     * @param blockID
     */
    public TaoBlock(long blockID) {
        mID = blockID;
        mData = new byte[TaoConfigs.BLOCK_SIZE];
        mTag = new Tag();
    }

    @Override
    public void initFromBlock(Block b) {
        mID = b.getBlockID();
        mData = b.getData();
        mTag = b.getTag();
    }

    @Override
    public void initFromSerialized(byte[] serialized) {
        try {
            mID = Longs.fromByteArray(Arrays.copyOfRange(serialized, 0, 8));
            mTag = new Tag();
            mTag.initFromSerialized(Arrays.copyOfRange(serialized, 8, 18));
            mData = new byte[TaoConfigs.BLOCK_SIZE];
            System.arraycopy(serialized, TaoConfigs.BLOCK_META_DATA_SIZE, mData, 0, TaoConfigs.BLOCK_SIZE);
        } catch (Exception e) {
            mID = -1;
            mData = new byte[TaoConfigs.BLOCK_SIZE];
            System.out.println("ERROR: unable to deserialize block");
        }
    }

    @Override
    public byte[] getData() {
        if (mData != null) {
            byte[] returnData = new byte[TaoConfigs.BLOCK_SIZE];
            System.arraycopy(mData, 0, returnData, 0, TaoConfigs.BLOCK_SIZE);
            return returnData;
        }

        return null;
    }

    @Override
    public void setData(byte[] data) {
        if (data != null) {
            System.arraycopy(data, 0, mData, 0, TaoConfigs.BLOCK_SIZE);
        } else {
            mData = null;
        }
    }

    @Override
    public long getBlockID() {
        return mID;
    }

    @Override
    public void setBlockID(long blockID) {
        mID = blockID;
    }

    @Override
    public Tag getTag() {
        return mTag;
    }

    @Override
    public void setTag(Tag tag) {
        mTag = tag;
    }

    @Override
    public Block getCopy() {
        Block b = new TaoBlock();
        b.initFromBlock(this);
        return b;
    }

    @Override
    public byte[] serialize() {
        byte[] idBytes = Longs.toByteArray(mID);
        byte[] tagBytes = mTag.serialize();
        return Bytes.concat(idBytes, tagBytes, mData);
    }

    @Override
    public boolean equals(Object obj) {
        if ( ! (obj instanceof Block) ) {
            return false;
        }

        if (obj == this) {
            return true;
        }

        // Two blocks are equal if they have the same blockID
        Block rhs = (Block) obj;
        if (mID != rhs.getBlockID()) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        // TODO: add data to hash?
        return Objects.hash(mID);
    }
}

package TaoProxy;

import TaoClient.OperationID;
import Messages.*;
import Configuration.TaoConfigs;

import com.google.common.primitives.Bytes;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.*;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;

public class TaoInterface {
    // Maps from operation ID to block ID
    public Map<OperationID, Long> mIncompleteCache = new HashMap<>();

    // Maps from block ID to how many ongoing operations involve that block.
    // This must be changed each time an operation is added to
    // or removed from the incompleteCache. When the number drops to 0,
    // the block ID must be removed from the map.
    public Map<Long, Integer> mBlocksInCache = new HashMap<>();

    protected LinkedList<OperationID> cacheOpsInOrder = new LinkedList<>();

    protected ReadWriteLock cacheLock = new ReentrantReadWriteLock();

    protected Sequencer mSequencer;

    protected TaoProcessor mProcessor;

    protected MessageCreator mMessageCreator;

    public TaoInterface(Sequencer s, TaoProcessor p, MessageCreator messageCreator) {
        mSequencer = s;
        mProcessor = p;
        mMessageCreator = messageCreator;
    }

    protected void removeOpFromCache(OperationID opID) {
        Long blockID = mIncompleteCache.get(opID);
        mIncompleteCache.remove(opID, blockID);
        System.out.println("Removed entry from cache");
        mBlocksInCache.put(blockID, mBlocksInCache.getOrDefault(blockID, 0) - 1);
        if (mBlocksInCache.get(blockID) <= 0) {
            mBlocksInCache.remove(blockID);
        }
        cacheOpsInOrder.remove(opID);
    }

    public void handleRequest(ClientRequest clientReq) {
        OperationID opID = clientReq.getOpID();
        int type = clientReq.getType();
        long blockID = clientReq.getBlockID();
        System.out.println("Got a request with opID " + opID);

        if (type == MessageTypes.CLIENT_READ_REQUEST) {
            cacheLock.writeLock().lock();

            // Evict oldest entry if cache is full
            while (mIncompleteCache.size() >= TaoConfigs.INCOMPLETE_CACHE_LIMIT) {
                System.out.println("Cache size: "+mIncompleteCache.size());
                System.out.println("Cache limit: "+TaoConfigs.INCOMPLETE_CACHE_LIMIT);
                OperationID opToRemove = cacheOpsInOrder.poll();
                System.out.println("Evicting "+opToRemove);
                removeOpFromCache(opToRemove);
            }

            mIncompleteCache.put(opID, blockID);
            mBlocksInCache.put(blockID, mBlocksInCache.getOrDefault(blockID, 0) + 1);
            System.out.println("There are now "+mBlocksInCache.get(blockID)+" instances of block " + blockID + " in the incomplete cache");
            cacheOpsInOrder.add(opID);
            cacheLock.writeLock().unlock();

            // If the block we just added to the incomplete cache exists in the subtree,
            // move it to the stash
            Bucket targetBucket = mProcessor.mSubtree.getBucketWithBlock(blockID);
            if (targetBucket != null) {
                targetBucket.lockBucket();
                HashSet<Long> blockIDToRemove = new HashSet<>();
                blockIDToRemove.add(blockID);
                Block b = targetBucket.removeBlocksInSet(blockIDToRemove).get(0);
                mProcessor.mSubtree.removeBlock(blockID);
                targetBucket.unlockBucket();
                mProcessor.mStash.addBlock(b);
                System.out.println("Moved block " + blockID + "from subtree to stash");
            }

            mSequencer.onReceiveRequest(clientReq);
        } else if (type == MessageTypes.CLIENT_WRITE_REQUEST) {
            cacheLock.readLock().lock();
            if (!mIncompleteCache.keySet().contains(opID)) {
                System.out.println("mIncompleteCache does not contain opID "+opID+"!");
                System.out.println(mIncompleteCache.keySet());
                cacheLock.readLock().unlock();
                return;
            } else {
                System.out.println("Found opID "+opID+" in cache");
            }
            // Update block in tree
            System.out.println("About to write to block");
            mProcessor.writeDataToBlock(blockID, clientReq.getData(), clientReq.getTag());
            System.out.println("Wrote data to block");
            cacheLock.readLock().unlock();

            // Remove operation from incomplete cache
            cacheLock.writeLock().lock();
            removeOpFromCache(opID);
            cacheLock.writeLock().unlock();

            // Create a ProxyResponse

            ProxyResponse response = mMessageCreator.createProxyResponse();
            response.setClientRequestID(clientReq.getRequestID());
            response.setWriteStatus(true);
            response.setReturnTag(new Tag());

            // Get channel
            AsynchronousSocketChannel clientChannel = clientReq.getChannel();

            // Create a response to send to client
            byte[] serializedResponse = response.serialize();
            byte[] header = MessageUtility.createMessageHeaderBytes(MessageTypes.PROXY_RESPONSE, serializedResponse.length);
            ByteBuffer fullMessage = ByteBuffer.wrap(Bytes.concat(header, serializedResponse));

            // Make sure only one response is sent at a time
            synchronized (clientChannel) {
                // Send message
                while (fullMessage.remaining() > 0) {
                    Future<Integer> writeResult = clientChannel.write(fullMessage);
                    try {
                        writeResult.get();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    //System.out.println("Replied to client");
                }

                // Clear buffer
                fullMessage = null;
            }
        }
    }

}

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
        TaoLogger.logInfo("Removed entry from cache");
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
        TaoLogger.logInfo("Got a request with opID " + opID);

        if (type == MessageTypes.CLIENT_READ_REQUEST) {
            cacheLock.writeLock().lock();

            // Evict oldest entry if cache is full
            while (mIncompleteCache.size() >= TaoConfigs.INCOMPLETE_CACHE_LIMIT) {
                TaoLogger.logInfo("Cache size: "+mIncompleteCache.size());
                TaoLogger.logInfo("Cache limit: "+TaoConfigs.INCOMPLETE_CACHE_LIMIT);
                OperationID opToRemove = cacheOpsInOrder.poll();
                TaoLogger.logInfo("Evicting "+opToRemove);
                removeOpFromCache(opToRemove);
            }

            mIncompleteCache.put(opID, blockID);
            mBlocksInCache.put(blockID, mBlocksInCache.getOrDefault(blockID, 0) + 1);
            TaoLogger.logInfo("There are now "+mBlocksInCache.get(blockID)+" instances of block " + blockID + " in the incomplete cache");
            cacheOpsInOrder.add(opID);
            cacheLock.writeLock().unlock();

            // If the block we just added to the incomplete cache exists in the subtree,
            // move it to the stash
            /*
            Bucket targetBucket = mProcessor.mSubtree.getBucketWithBlock(blockID);
            if (targetBucket != null) {
                targetBucket.lockBucket();
                HashSet<Long> blockIDToRemove = new HashSet<>();
                blockIDToRemove.add(blockID);
                Block b = targetBucket.removeBlocksInSet(blockIDToRemove).get(0);
                mProcessor.mSubtree.removeBlock(blockID);
                targetBucket.unlockBucket();
                mProcessor.mStash.addBlock(b);
                TaoLogger.logInfo("Moved block " + blockID + "from subtree to stash");
            }
            */

            mSequencer.onReceiveRequest(clientReq);
        } else if (type == MessageTypes.CLIENT_WRITE_REQUEST) {
            cacheLock.readLock().lock();
            if (!mIncompleteCache.keySet().contains(opID)) {
                TaoLogger.logInfo("mIncompleteCache does not contain opID "+opID+"!");
                TaoLogger.logInfo(mIncompleteCache.keySet().toString());
                cacheLock.readLock().unlock();
                return;
            } else {
                TaoLogger.logInfo("Found opID "+opID+" in cache");
            }
            // if this was a dummy request from the daemon we don't want to overwrite the block
            if(clientReq.getRequestID() != -1) {
				// Update block in tree
				TaoLogger.logInfo("About to write to block");
				mProcessor.writeDataToBlock(blockID, clientReq.getData(), clientReq.getTag());
				TaoLogger.logInfo("Wrote data to block");
            }
			// this needs to be here so that we can guarantee the block doesn't get deleted
            // before we write to it
            cacheLock.readLock().unlock();

            // Remove operation from incomplete cache
            cacheLock.writeLock().lock();
            removeOpFromCache(opID);
            cacheLock.writeLock().unlock();
            
			// update path timestamps
            long pathID = mProcessor.mPositionMap.getBlockPosition(blockID);
            if (pathID == -1) {
            	TaoLogger.logForce("Path ID for blockID " + blockID
            			+ " was unmapped during o_write. This should never happen!");
            	System.exit(1);
            }
            // queues this path to be written back and updates the block timestamp
            mProcessor.update_timestamp(pathID);


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

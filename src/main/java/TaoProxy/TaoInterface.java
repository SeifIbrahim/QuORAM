package TaoProxy;

import TaoClient.OperationID;
import Messages.*;
import Configuration.TaoConfigs;

import com.google.common.primitives.Bytes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.*;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;

public class TaoInterface {
	// Maps from operation ID to block ID
	public Map<OperationID, Long> mIncompleteCache = new ConcurrentHashMap<>();

	// Maps from block ID to how many ongoing operations involve that block.
	// This must be changed each time an operation is added to
	// or removed from the incompleteCache. When the number drops to 0,
	// the block ID must be removed from the map.
	public Map<Long, Integer> mBlocksInCache = new ConcurrentHashMap<>();

	protected Queue<OperationID> cacheOpsInOrder = new ArrayBlockingQueue<>(TaoConfigs.INCOMPLETE_CACHE_LIMIT);

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
		if (!mIncompleteCache.remove(opID, blockID)) {
			TaoLogger.logForce("Removing entry from cache failed");
		}
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
		((TaoProcessor) mProcessor).mProfiler.reachedInterface(clientReq);

		if (type == MessageTypes.CLIENT_READ_REQUEST) {
			TaoLogger.logInfo("Got a read request with opID " + opID);

			ArrayList<Long> evictedPathIDs = new ArrayList<Long>();

			// Evict oldest entry if cache is full
			while (mIncompleteCache.size() >= TaoConfigs.INCOMPLETE_CACHE_LIMIT) {
				TaoLogger.logInfo("Cache size: " + mIncompleteCache.size());
				TaoLogger.logInfo("Cache limit: " + TaoConfigs.INCOMPLETE_CACHE_LIMIT);
				OperationID opToRemove = cacheOpsInOrder.poll();
				TaoLogger.logInfo("Evicting " + opToRemove);
				long evictedBlockID = mIncompleteCache.get(opID);
				long evictedPathID = mProcessor.mPositionMap.getBlockPosition(evictedBlockID);
				evictedPathIDs.add(evictedPathID);
				removeOpFromCache(opToRemove);
			}

			mIncompleteCache.put(opID, blockID);
			mBlocksInCache.put(blockID, mBlocksInCache.getOrDefault(blockID, 0) + 1);
			TaoLogger.logInfo("There are now " + mBlocksInCache.get(blockID) + " instances of block " + blockID
					+ " in the incomplete cache");
			cacheOpsInOrder.add(opID);

			// add the paths for the evicted blocks to the write queue and increment # paths
			// this is to prevent unbounded memory in the case that no o_write ever comes to
			// trigger writeback
			for (Long evictedPathID : evictedPathIDs) {
				if (evictedPathID != -1) {
					// Add this path to the write queue
					synchronized (mProcessor.mWriteQueue) {
						TaoLogger.logInfo(
								"Adding " + evictedPathID + " to mWriteQueue due to incomplete cache eviction");
						mProcessor.mWriteQueue.add(evictedPathID);
					}
					mProcessor.mWriteBackCounter.getAndIncrement();
					mProcessor.writeBack();
				}
			}

			// If the block we just added to the incomplete cache exists in the subtree,
			// move it to the stash
			/*
			 * Bucket targetBucket = mProcessor.mSubtree.getBucketWithBlock(blockID); if
			 * (targetBucket != null) { targetBucket.lockBucket(); HashSet<Long>
			 * blockIDToRemove = new HashSet<>(); blockIDToRemove.add(blockID); Block b =
			 * targetBucket.removeBlocksInSet(blockIDToRemove).get(0);
			 * mProcessor.mSubtree.removeBlock(blockID); targetBucket.unlockBucket();
			 * mProcessor.mStash.addBlock(b); TaoLogger.logInfo("Moved block " + blockID +
			 * "from subtree to stash"); }
			 */

			mSequencer.onReceiveRequest(clientReq);
			// readPath needs to happen AFTER we forward to the sequencer
			mProcessor.readPath(clientReq);
		} else if (type == MessageTypes.CLIENT_WRITE_REQUEST) {
			TaoLogger.logInfo("Got a write request with opID " + opID);
			// Create a ProxyResponse
			ProxyResponse response = mMessageCreator.createProxyResponse();
			response.setClientRequestID(clientReq.getRequestID());
			response.setWriteStatus(true);
			response.setReturnTag(new Tag());
			if (!mIncompleteCache.keySet().contains(opID)) {
				TaoLogger.logInfo("mIncompleteCache does not contain opID " + opID + "!");
				TaoLogger.logInfo(mIncompleteCache.keySet().toString());
				response.setFailed(true);
			} else {
				TaoLogger.logInfo("Found opID " + opID + " in cache");

				// if this was a dummy request from the daemon we don't want to overwrite the
				// block
				if (clientReq.getRequestID() != -1) {
					// Update block in tree
					TaoLogger.logInfo("About to write to block");
					mProcessor.writeDataToBlock(blockID, clientReq.getData(), clientReq.getTag());
					TaoLogger.logInfo("Wrote data to block");
				}

				// Remove operation from incomplete cache
				removeOpFromCache(opID);

				// queues this path to be written back and updates the block timestamp
				mProcessor.update_timestamp(blockID);
			}

			// Get channel
			AsynchronousSocketChannel clientChannel = clientReq.getChannel();

			long processingTime = ((TaoProcessor) mProcessor).mProfiler.proxyOperationComplete(clientReq);
			response.setProcessingTime(processingTime);

			// Create a response to send to client
			byte[] serializedResponse = response.serialize();
			byte[] header = MessageUtility.createMessageHeaderBytes(MessageTypes.PROXY_RESPONSE,
					serializedResponse.length);
			ByteBuffer fullMessage = ByteBuffer.wrap(Bytes.concat(header, serializedResponse));

			// Make sure only one response is sent at a time
			synchronized (clientChannel) {
				// Send message
				while (fullMessage.remaining() > 0) {
					Future<Integer> writeResult = clientChannel.write(fullMessage);
					try {
						writeResult.get();
					} catch (Exception e) {
						e.printStackTrace(System.out);
					}
					// System.out.println("Replied to client");
				}

				// Clear buffer
				fullMessage = null;
			}
		}
	}

}

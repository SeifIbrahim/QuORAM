package TaoProxy;

import Configuration.TaoConfigs;
import Messages.*;
import com.google.common.primitives.Bytes;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @brief The Sequencer makes sure that replies are sent to the client in the same order that requests were received
 */
public class TaoSequencer implements Sequencer {
	// Size of request queue
	protected final static int QUEUE_SIZE = 100000;

	// Map that will map each request to the value of the requested block
	// The value will be null if the reply to this request has not yet been received
	protected Map<ClientRequest, Block> mRequestMap;

	// Queue of the received requests
	protected BlockingQueue<ClientRequest> mRequestQueue;

	// Path and message creators
	protected PathCreator mBlockCreator;
	protected MessageCreator mMessageCreator;

	// The channel group used for asynchronous socket
	protected AsynchronousChannelGroup mThreadGroup;

	// A map of each client to a channel to that client
	protected Map<InetSocketAddress, AsynchronousSocketChannel> mChannelMap;

	public Processor mProcessor;

	/**
	 * @brief Default constructor for the TaoStore Sequencer
	 */
	public TaoSequencer(MessageCreator messageCreator, PathCreator pathCreator) {
		try {
			// Assign message creator
			mMessageCreator = messageCreator;

			// Assign path creator
			mBlockCreator = pathCreator;

			// NOTE: ConcurrentHashMap is weakly consistent amongst different threads.
			// Should be fine in this scenario
			mRequestMap = new ConcurrentHashMap<>();

			// Initialize the request queue with QUEUE_SIZE
			mRequestQueue = new ArrayBlockingQueue<>(QUEUE_SIZE);

			// Initialize channel map
			mChannelMap = new HashMap<>();

			// Thread group used for asynchronous I/O
			mThreadGroup = AsynchronousChannelGroup.withFixedThreadPool(TaoConfigs.PROXY_THREAD_COUNT,
					Executors.defaultThreadFactory());

			// Run the serialize procedure in a different thread
			Runnable serializeProcedure = this::serializationProcedure;
			new Thread(serializeProcedure).start();
		} catch (Exception e) {
			e.printStackTrace(System.out);
		}
	}

	@Override
	public void onReceiveRequest(ClientRequest req) {
		try {
			// Create an empty block with null data
			Block empty = mBlockCreator.createBlock();
			empty.setData(null);

			// Put request and new empty block into request map
			mRequestMap.put(req, empty);

			// Add this request to the request queue
			mRequestQueue.add(req);

			TaoLogger.logDebug("Added requestID " + req.getRequestID() + " to sequencer");
		} catch (Exception e) {
			e.printStackTrace(System.out);
		}
	}

	@Override
	public void onReceiveResponse(ClientRequest req, ServerResponse resp, byte[] data, Tag tag) {
		try {
			// Create a new block and set the data
			Block b = mBlockCreator.createBlock();
			b.setData(data);
			b.setTag(tag);

			// Replace empty null block with new block and notify serializationProcedure
			synchronized (mRequestMap) {
				mRequestMap.replace(req, b);
				mRequestMap.notify();
			}

			TaoLogger.logDebug("Sequencer finished onReceiveResponse for " + req.getRequestID());
		} catch (Exception e) {
			e.printStackTrace(System.out);
		}
	}

	@Override
	public void serializationProcedure() {
		while (true) {
			try {
				// Retrieve request from request queue
				// Blocks if there is no item in queue
				ClientRequest req = mRequestQueue.take();

				// Wait until the reply for req comes back
				byte[] check;
				synchronized (mRequestMap) {
					if (mRequestMap.get(req) == null) {
						continue;
					}
					check = mRequestMap.get(req).getData();
					while (check == null) {
						mRequestMap.wait();
						check = mRequestMap.get(req).getData();
						TaoLogger.logDebug("Top of requestQueue id:" + req.getRequestID());
					}
				}

				TaoLogger.logDebug(
						"1 Sequencer going to send response for " + req.getRequestID() + " that was for blockid "
								+ req.getBlockID() + " hostname " + req.getClientAddress().getHostName());

				// Create a ProxyResponse based on type of request
				ProxyResponse response = null;
				if (req.getType() == MessageTypes.CLIENT_READ_REQUEST) {
					// System.out.println("Answering read request");
					response = mMessageCreator.createProxyResponse();
					response.setClientRequestID(req.getRequestID());
					response.setReturnData(mRequestMap.get(req).getData());
					response.setReturnTag(mRequestMap.get(req).getTag());
				} else if (req.getType() == MessageTypes.CLIENT_WRITE_REQUEST) {
					// System.out.println("Answering write request");
					response = mMessageCreator.createProxyResponse();
					response.setClientRequestID(req.getRequestID());
					response.setWriteStatus(true);
					response.setReturnTag(mRequestMap.get(req).getTag());
				}
				long processingTime = ((TaoProcessor) mProcessor).mProfiler.proxyOperationComplete(req);
				response.setProcessingTime(processingTime);

				// Get channel
				AsynchronousSocketChannel clientChannel = req.getChannel();

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
						writeResult.get();
						// System.out.println("Replied to client");
					}

					// Clear buffer
					fullMessage = null;
				}
				// Remove request from request map
				mRequestMap.remove(req);
			} catch (Exception e) {
				e.printStackTrace(System.out);
			}
		}
	}
}

package TaoProxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import com.google.common.primitives.Bytes;

import Configuration.TaoConfigs;
import Configuration.Unit;
import Configuration.Utility;
import Messages.ClientRequest;
import Messages.MessageCreator;
import Messages.MessageTypes;
import Messages.ProxyRequest;
import Messages.ServerResponse;

public class TaoProcessor implements Processor {
	// Stash to hold blocks
	public Stash mStash;

	// Map that maps a block ID to a list of requests for that block ID
	// Used so that we know when to issue fake reads (fake reads are issued if the
	// list for a requestID is non empty)
	protected Map<Long, List<ClientRequest>> mRequestMap;

	// A map that maps each blockID to a read/write lock. These locks are used to
	// provide read access to a
	// list of requests for a blockID (obtained from mRequestMap) during a readPath
	// call, and will be used to grant
	// write access during an answerRequest call. This is needed because there is a
	// race condition in the presence of
	// multiple concurrent requests where answerRequest is going through the list of
	// requests in a particular blockID
	// list, and at the same time an invocation of readPath on the same blockID is
	// occurring. In this scenario, the
	// readPath method may assign that blockID to be a fake read, and before adding
	// the request to the request list the
	// answerRequest method will finish execution. Thus the fake read will not be
	// responded to after is is inserted into
	// the request list
	protected Map<Long, ReentrantReadWriteLock> mRequestLockMap;

	// Map that maps client requests to a ResponseMapEntry, signifying whether or
	// not a request has been received or not
	protected Map<ClientRequest, ResponseMapEntry> mResponseMap;

	// MultiSet which keeps track of which paths have been requested but not yet
	// returned
	// This is needed for write back, to know what should or should not be deleted
	// from the subtree when write completes
	protected Multiset<Long> mPathReqMultiSet;

	// Subtree
	public Subtree mSubtree;

	// Counter used to know when we should writeback
	protected AtomicLong mWriteBackCounter;

	// Timestamp that is incremented whenever we update the subtree
	protected AtomicLong mTimestamp;

	// Used to keep track of when the next writeback should occur
	// When mWriteBackCounter == mNextWriteBack, a writeback should occur
	protected long mNextWriteBack;

	// Used to make sure that the writeback is only executed by one thread
	protected final transient ReentrantLock mWriteBackLock = new ReentrantLock();

	// Used to prevent flushing while a snapshot of the subtree is taken for
	// writeback
	protected final transient ReentrantReadWriteLock mSubtreeRWL = new ReentrantReadWriteLock();

	// Write queue used to store which paths should be sent to server on next
	// writeback
	protected Queue<Long> mWriteQueue;

	// Position map which keeps track of what leaf each block corresponds to
	protected PositionMap mPositionMap;

	// Proxy that this processor belongs to
	protected Proxy mProxy;

	// Sequencer that belongs to the proxy
	protected Sequencer mSequencer;

	// The channel group used for asynchronous socket
	protected AsynchronousChannelGroup mThreadGroup;

	// CryptoUtil used for encrypting and decrypting paths
	protected CryptoUtil mCryptoUtil;

	// MessageCreator for creating different types of messages
	protected MessageCreator mMessageCreator;

	// PathCreator responsible for making empty blocks, buckets, and paths
	protected PathCreator mPathCreator;

	// A map that maps each leafID to the relative leaf ID it would have within a
	// server partition
	protected Map<Long, Long> mRelativeLeafMapper;

	// Map each client to a map that map's each storage server's InetSocketAddress
	// to a channel to that storage server
	// We use this in order to have at least one dedicated channel for each client
	// to each storage server to increase throughput,
	// as creating a new channel each time can result in a lot of overhead
	// In the case the the dedicated channel is occupied, we will just create a new
	// channel
	protected Map<AsynchronousSocketChannel, Map<InetSocketAddress, AsynchronousSocketChannel>> mProxyToServerChannelMap;

	// Each client will have a map that maps each storage server's InetSocketAddress
	// to a semaphore
	// This semaphore will control access to the dedicated channel so that two
	// threads aren't using it at the same time
	protected Map<AsynchronousSocketChannel, Map<InetSocketAddress, Semaphore>> mAsyncProxyToServerSemaphoreMap;

	// The Profiler to store timing information
	protected Profiler mProfiler;

	protected int mUnitId;

	public TaoInterface mInterface;

	private long mLoadTestStartTime = System.currentTimeMillis();

	// time interval for bucketting the average number of blocks in the proxy
	private final int NUM_BLOCKS_SAMPLE_INTERVAL = 10 * 1000;

	// number of blocks in the proxy bucketed by time interval
	public ArrayList<ArrayList<Long>> mBucketedSubtreeBlocks = new ArrayList<ArrayList<Long>>();
	public ArrayList<ArrayList<Long>> mBucketedStashBlocks = new ArrayList<ArrayList<Long>>();
	public ArrayList<ArrayList<Long>> mBucketedOrphanBlocks = new ArrayList<ArrayList<Long>>();

	private boolean mDoingLoadTest = false;

	private Executor mProcessorThreadPool;

	// Holds blocks that were deleted from the subtree before the write came
	private ConcurrentMap<Long, Block> mOrphanBlocks;

	/**
	 * @brief Constructor
	 * @param proxy
	 * @param sequencer
	 * @param threadGroup
	 * @param messageCreator
	 * @param pathCreator
	 * @param cryptoUtil
	 * @param subtree
	 * @param positionMap
	 * @param relativeMapper
	 */
	public TaoProcessor(Proxy proxy, Sequencer sequencer, AsynchronousChannelGroup threadGroup,
			MessageCreator messageCreator, PathCreator pathCreator, CryptoUtil cryptoUtil, Subtree subtree,
			PositionMap positionMap, Map<Long, Long> relativeMapper, Profiler profiler, int unitId) {
		try {
			// The ORAM unit that this processor belongs to
			mUnitId = unitId;

			// The proxy that this processor belongs to
			mProxy = proxy;

			// The sequencer that belongs to the proxy
			mSequencer = sequencer;

			// Thread group used for asynchronous I/O
			mThreadGroup = AsynchronousChannelGroup.withFixedThreadPool(TaoConfigs.PROXY_THREAD_COUNT,
					Executors.defaultThreadFactory());

			// Assign message creator
			mMessageCreator = messageCreator;

			// Assign path creator
			mPathCreator = pathCreator;

			// Assign crypto util
			mCryptoUtil = cryptoUtil;

			// Create stash
			mStash = new TaoStash();

			// Create request map
			mRequestMap = new ConcurrentHashMap<>();
			mRequestLockMap = new ConcurrentHashMap<>();

			// Create response map
			mResponseMap = new ConcurrentHashMap<>();

			// Create requested path multiset
			mPathReqMultiSet = ConcurrentHashMultiset.create();

			mOrphanBlocks = new ConcurrentHashMap<>();

			// Assign subtree
			mSubtree = subtree;
			((TaoSubtree) subtree).mOrphanBlocks = mOrphanBlocks;

			// Create counter the keep track of number of flushes
			mWriteBackCounter = new AtomicLong();

			mTimestamp = new AtomicLong();

			mNextWriteBack = TaoConfigs.WRITE_BACK_THRESHOLD;

			// Create list of queues of paths to be written
			// The index into the list corresponds to the server at that same index in
			// TaoConfigs.PARTITION_SERVERS
			mWriteQueue = new ConcurrentLinkedQueue<>();

			// Create position map
			mPositionMap = positionMap;

			// Map each leaf to a relative leaf for the servers
			mRelativeLeafMapper = relativeMapper;

			mProfiler = profiler;

			// Initialize maps
			mProxyToServerChannelMap = new ConcurrentHashMap<>();
			mAsyncProxyToServerSemaphoreMap = new ConcurrentHashMap<>();

			mProcessorThreadPool = Executors.newFixedThreadPool(TaoConfigs.PROXY_SERVICE_THREADS);
			;
		} catch (Exception e) {
			e.printStackTrace(System.out);
		}
	}

	private void logNumBlocks() {
		if (mDoingLoadTest) {
			while (mBucketedSubtreeBlocks.size() < (int) (1
					+ (System.currentTimeMillis() - mLoadTestStartTime) / NUM_BLOCKS_SAMPLE_INTERVAL)) {
				mBucketedSubtreeBlocks.add(new ArrayList<Long>());
			}
			mBucketedSubtreeBlocks
					.get((int) ((System.currentTimeMillis() - mLoadTestStartTime) / NUM_BLOCKS_SAMPLE_INTERVAL))
					.add(mSubtree.getNumBlocks());

			while (mBucketedStashBlocks.size() < (int) (1
					+ (System.currentTimeMillis() - mLoadTestStartTime) / NUM_BLOCKS_SAMPLE_INTERVAL)) {
				mBucketedStashBlocks.add(new ArrayList<Long>());
			}
			mBucketedStashBlocks
					.get((int) ((System.currentTimeMillis() - mLoadTestStartTime) / NUM_BLOCKS_SAMPLE_INTERVAL))
					.add((long) ((TaoStash) mStash).mStash.size());

			while (mBucketedOrphanBlocks.size() < (int) (1
					+ (System.currentTimeMillis() - mLoadTestStartTime) / NUM_BLOCKS_SAMPLE_INTERVAL)) {
				mBucketedOrphanBlocks.add(new ArrayList<Long>());
			}
			mBucketedOrphanBlocks
					.get((int) ((System.currentTimeMillis() - mLoadTestStartTime) / NUM_BLOCKS_SAMPLE_INTERVAL))
					.add((long) mOrphanBlocks.size());
		}
	}

	@Override
	public void readPath(ClientRequest req) {
		try {
			TaoLogger.logInfo(
					"Starting a readPath for blockID " + req.getBlockID() + " and request #" + req.getRequestID());

			// Create new entry into response map for this request
			mResponseMap.put(req, new ResponseMapEntry());

			// Variables needed for fake read check
			boolean fakeRead;
			long pathID;

			// We make sure the request list and read/write lock for maps are not null
			List<ClientRequest> requestList = new ArrayList<>();
			ReentrantReadWriteLock requestListLock = new ReentrantReadWriteLock();
			mRequestLockMap.putIfAbsent(req.getBlockID(), requestListLock);
			requestListLock = mRequestLockMap.get(req.getBlockID());

			// Acquire a read lock to ensure we do not assign a fake read that will not be
			// answered to
			requestListLock.writeLock().lock();

			mRequestMap.putIfAbsent(req.getBlockID(), requestList);
			requestList = mRequestMap.get(req.getBlockID());

			// Check if there is any current request for this block ID
			if (requestList.isEmpty()) {
				// If no other requests for this block ID have been made, it is not a fake read
				fakeRead = false;

				// Find the path that this block maps to
				pathID = mPositionMap.getBlockPosition(req.getBlockID());

				// If pathID is -1, that means that this blockID is not yet mapped to a path
				if (pathID == -1) {
					// Fetch a random path from server
					pathID = mCryptoUtil.getRandomPathID();
				}
			} else {
				// There is currently a request for the block ID, so we need to trigger a fake
				// read
				fakeRead = true;

				// Fetch a random path from server
				pathID = mCryptoUtil.getRandomPathID();
			}

			// Add request to request map list
			requestList.add(req);

			// Unlock
			requestListLock.writeLock().unlock();

			TaoLogger.logInfo("Doing a read for pathID " + pathID + " and block ID " + req.getBlockID() + "("
					+ mPositionMap.getBlockPosition(req.getBlockID()) + ")");

			// Insert request into mPathReqMultiSet to make sure that this path is not
			// deleted before this response
			// returns from server
			mPathReqMultiSet.add(pathID);

			// Create effectively final variables to use for inner classes
			long relativeFinalPathID = mRelativeLeafMapper.get(pathID);
			long absoluteFinalPathID = pathID;

			// Get the map for particular client that maps the client to the channels
			// connected to the server
			Map<InetSocketAddress, AsynchronousSocketChannel> mChannelMap = mProxyToServerChannelMap
					.get(req.getChannel());

			// If this is there first time the client has connected, the map may not have
			// been made yet
			if (mChannelMap == null) {
				TaoLogger.logInfo("Going to make the initial connections for " + req.getClientAddress().getHostName());
				makeInitialConnections(req.getChannel());
			}

			// Get it once more in case it was null the first time
			mChannelMap = mProxyToServerChannelMap.get(req.getChannel());

			// Do this to make sure we wait until connections are made if this is one of the
			// first requests made by this
			// particular client
			if (mChannelMap.size() < 1) {
				makeInitialConnections(req.getChannel());
			}

			// Get the particular server InetSocketAddress that we want to connect to
			InetSocketAddress targetServer = mPositionMap.getServerForPosition(pathID);

			// Get the channel to that server
			AsynchronousSocketChannel channelToServer = mChannelMap.get(targetServer);

			// Get the serverSemaphoreMap for this client, which will be used to control
			// access to the dedicated channel
			// when requests from the same client arrive for the same server
			Map<InetSocketAddress, Semaphore> serverSemaphoreMap = mAsyncProxyToServerSemaphoreMap
					.get(req.getChannel());

			// Create a read request to send to server
			ProxyRequest proxyRequest = mMessageCreator.createProxyRequest();
			proxyRequest.setPathID(relativeFinalPathID);
			proxyRequest.setType(MessageTypes.PROXY_READ_REQUEST);
			// use the timestamp as a unique id here for profiling on the server
			proxyRequest.setTimestamp(req.getRequestID());

			// Serialize request
			byte[] requestData = proxyRequest.serialize();

			mProfiler.readPathPreSend(targetServer, req);

			// Claim either the dedicated channel or create a new one if the dedicated
			// channel is already being used
			if (serverSemaphoreMap.get(targetServer).tryAcquire()) {
				// First we send the message type to the server along with the size of the
				// message
				byte[] messageType = MessageUtility.createMessageHeaderBytes(MessageTypes.PROXY_READ_REQUEST,
						requestData.length);
				ByteBuffer entireMessage = ByteBuffer.wrap(Bytes.concat(messageType, requestData));

				// Asynchronously send message type and length to server
				channelToServer.write(entireMessage, null, new CompletionHandler<Integer, Void>() {
					@Override
					public void completed(Integer result, Void attachment) {
						// Make sure we write the whole message
						while (entireMessage.remaining() > 0) {
							channelToServer.write(entireMessage, null, this);
							return;
						}

						// Asynchronously read response type and size from server
						ByteBuffer messageTypeAndSize = MessageUtility.createTypeReceiveBuffer();

						channelToServer.read(messageTypeAndSize, null, new CompletionHandler<Integer, Void>() {
							@Override
							public void completed(Integer result, Void attachment) {
								// profiling
								mProfiler.readPathPostRecv(targetServer, req);

								// Make sure we read the entire message
								while (messageTypeAndSize.remaining() > 0) {
									channelToServer.read(messageTypeAndSize, null, this);
									return;
								}

								// Flip the byte buffer for reading
								messageTypeAndSize.flip();

								// Parse the message type and size from server
								int[] typeAndLength = MessageUtility.parseTypeAndLength(messageTypeAndSize);
								int messageType = typeAndLength[0];
								int messageLength = typeAndLength[1];

								// Asynchronously read response from server
								ByteBuffer pathInBytes = ByteBuffer.allocate(messageLength);
								channelToServer.read(pathInBytes, null, new CompletionHandler<Integer, Void>() {
									@Override
									public void completed(Integer result, Void attachment) {
										// Make sure we read all the bytes for the path
										while (pathInBytes.remaining() > 0) {
											channelToServer.read(pathInBytes, null, this);
											return;
										}

										// Release the semaphore for this server's dedicated channel
										serverSemaphoreMap.get(targetServer).release();

										// Flip the byte buffer for reading
										pathInBytes.flip();

										// Serve message based on type
										if (messageType == MessageTypes.SERVER_RESPONSE) {
											// Get message bytes
											byte[] serialized = new byte[messageLength];
											pathInBytes.get(serialized);

											// Create ServerResponse object based on data
											ServerResponse response = mMessageCreator.createServerResponse();
											response.initFromSerialized(serialized);

											// Set absolute path ID
											response.setPathID(absoluteFinalPathID);

											long serverProcessingTime = response.getProcessingTime();
											mProfiler.readPathServerProcessingTime(targetServer, req,
													serverProcessingTime);

											mProfiler.readPathComplete(req);

											// Send response to proxy
											Runnable serializeProcedure = () -> mProxy.onReceiveResponse(req, response,
													fakeRead);
											mProcessorThreadPool.execute(serializeProcedure);
										}
									}

									@Override
									public void failed(Throwable exc, Void attachment) {
										exc.printStackTrace(System.out);
									}
								});
							}

							@Override
							public void failed(Throwable exc, Void attachment) {
								exc.printStackTrace(System.out);
							}
						});
					}

					@Override
					public void failed(Throwable exc, Void attachment) {
						exc.printStackTrace(System.out);
					}
				});
			} else {
				// We could not claim the dedicated channel for this server, we will instead
				// create a new channel

				// Get the channel to that server
				AsynchronousSocketChannel newChannelToServer = AsynchronousSocketChannel.open(mThreadGroup);

				// Connect to server
				newChannelToServer.connect(targetServer, null, new CompletionHandler<Void, Object>() {
					@Override
					public void completed(Void result, Object attachment) {
						// First we send the message type to the server along with the size of the
						// message
						byte[] messageType = MessageUtility.createMessageHeaderBytes(MessageTypes.PROXY_READ_REQUEST,
								requestData.length);
						ByteBuffer entireMessage = ByteBuffer.wrap(Bytes.concat(messageType, requestData));

						// Asynchronously send message type and length to server
						newChannelToServer.write(entireMessage, null, new CompletionHandler<Integer, Void>() {
							@Override
							public void completed(Integer result, Void attachment) {
								// Make sure we write the whole message
								while (entireMessage.remaining() > 0) {
									newChannelToServer.write(entireMessage, null, this);
									return;
								}

								// Asynchronously read response type and size from server
								ByteBuffer messageTypeAndSize = MessageUtility.createTypeReceiveBuffer();

								newChannelToServer.read(messageTypeAndSize, null,
										new CompletionHandler<Integer, Void>() {
											@Override
											public void completed(Integer result, Void attachment) {
												// profiling
												mProfiler.readPathPostRecv(targetServer, req);

												// Make sure we read the entire message
												while (messageTypeAndSize.remaining() > 0) {
													newChannelToServer.read(messageTypeAndSize, null, this);
													return;
												}

												// Flip the byte buffer for reading
												messageTypeAndSize.flip();

												// Parse the message type and size from server
												int[] typeAndLength = MessageUtility
														.parseTypeAndLength(messageTypeAndSize);
												int messageType = typeAndLength[0];
												int messageLength = typeAndLength[1];

												// Asynchronously read response from server
												ByteBuffer pathInBytes = ByteBuffer.allocate(messageLength);
												newChannelToServer.read(pathInBytes, null,
														new CompletionHandler<Integer, Void>() {
															@Override
															public void completed(Integer result, Void attachment) {
																// Make sure we read all the bytes for the path
																while (pathInBytes.remaining() > 0) {
																	newChannelToServer.read(pathInBytes, null, this);
																	return;
																}

																// Close channel
																try {
																	newChannelToServer.close();
																} catch (IOException e) {
																	e.printStackTrace(System.out);
																}

																// Flip the byte buffer for reading
																pathInBytes.flip();

																// Serve message based on type
																if (messageType == MessageTypes.SERVER_RESPONSE) {
																	// Get message bytes
																	byte[] serialized = new byte[messageLength];
																	pathInBytes.get(serialized);

																	// Create ServerResponse object based on data
																	ServerResponse response = mMessageCreator
																			.createServerResponse();
																	response.initFromSerialized(serialized);

																	// Set absolute path ID
																	response.setPathID(absoluteFinalPathID);

																	long serverProcessingTime = response
																			.getProcessingTime();
																	mProfiler.readPathServerProcessingTime(targetServer,
																			req, serverProcessingTime);

																	mProfiler.readPathComplete(req);

																	// Send response to proxy
																	Runnable serializeProcedure = () -> mProxy
																			.onReceiveResponse(req, response, fakeRead);
																	mProcessorThreadPool.execute(serializeProcedure);
																}
															}

															@Override
															public void failed(Throwable exc, Void attachment) {
																exc.printStackTrace(System.out);
															}
														});
											}

											@Override
											public void failed(Throwable exc, Void attachment) {
												exc.printStackTrace(System.out);
											}
										});
							}

							@Override
							public void failed(Throwable exc, Void attachment) {
								exc.printStackTrace(System.out);
							}
						});
					}

					@Override
					public void failed(Throwable exc, Object attachment) {
						exc.printStackTrace(System.out);
					}
				});
			}
		} catch (Exception e) {
			e.printStackTrace(System.out);
		}
	}

	/**
	 * @brief Private helper method to make a map of server addresses to channels for addr
	 * @param addr
	 */
	protected void makeInitialConnections(AsynchronousSocketChannel addr) {
		try {
			// Get the number of storage servers
			int numServers = 1;

			// Create a new map
			Map<InetSocketAddress, AsynchronousSocketChannel> newMap = new HashMap<>();

			// Atomically add channel map if not present
			mProxyToServerChannelMap.putIfAbsent(addr, newMap);

			// Get map in case two threads attempted to add the map at the same time
			newMap = mProxyToServerChannelMap.get(addr);

			// Claim map
			synchronized (newMap) {
				// Check if another thread has already added channels
				if (newMap.size() != numServers) {
					// Create the taken semaphore for this addr
					Map<InetSocketAddress, Semaphore> newSemaphoreMap = new ConcurrentHashMap<>();
					mAsyncProxyToServerSemaphoreMap.put(addr, newSemaphoreMap);

					// Create the channels to the storage servers
					for (int i = 0; i < numServers; i++) {
						AsynchronousSocketChannel channel = AsynchronousSocketChannel.open(mThreadGroup);
						Unit u = TaoConfigs.ORAM_UNITS.get(mUnitId);
						InetSocketAddress serverAddr = new InetSocketAddress(u.serverHost, u.serverPort);
						Future<?> connection = channel.connect(serverAddr);
						connection.get();

						// Add the channel to the map
						newMap.put(serverAddr, channel);

						// Add a new semaphore for this channel to the map
						newSemaphoreMap.put(serverAddr, new Semaphore(1));
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace(System.out);
		}
	}

	@Override
	public void answerRequest(ClientRequest req, ServerResponse resp, boolean isFakeRead) {
		TaoLogger.logInfo("Going to answer request with requestID " + req.getRequestID() + " from host "
				+ req.getClientAddress().getHostName());

		// Get information about response
		boolean fakeRead = isFakeRead;

		// Decrypt response data
		byte[] encryptedPathBytes = resp.getPathBytes();
		Path decryptedPath = mCryptoUtil.decryptPath(encryptedPathBytes);

		// Set the correct path ID
		decryptedPath.setPathID(resp.getPathID());

		// Profiling
		long preAddPathTime = System.currentTimeMillis();

		// Insert every bucket along path that is not in subtree into subtree
		// We update the timestamp at this point in order to ensure that a delete does
		// not delete an ancestor node
		// of a node that has been flushed to
		mSubtreeRWL.writeLock().lock();
		mSubtree.addPath(decryptedPath, mTimestamp.get());
		mSubtreeRWL.writeLock().unlock();

		// Profiling
		mProfiler.addPathTime(System.currentTimeMillis() - preAddPathTime);

		// Update the response map entry for this request, marking it as returned
		ResponseMapEntry responseMapEntry = mResponseMap.get(req);
		responseMapEntry.setReturned(true);

		// Check if the data for this response entry is not null, which would be the
		// case if the real read returned
		// before this fake read
		if (responseMapEntry.getData() != null) {
			TaoLogger.logDebug("answerRequest requestID " + req.getRequestID() + " from host "
					+ req.getClientAddress().getHostName() + " was a fake read, and real read has responded earlier");
			// The real read has already appeared, so we can answer the client

			mProfiler.answerRequestComplete(req);

			// Send the data to the sequencer
			mSequencer.onReceiveResponse(req, resp, responseMapEntry.getData(), responseMapEntry.getTag());

			// Remove this request from the response map
			mResponseMap.remove(req);

			// We are done
			return;
		}

		// log the number of blocks in the subtree
		logNumBlocks();

		// If the data has not yet returned, we check to see if this is the request that
		// caused the real read for this block
		if (!fakeRead) {
			TaoLogger.logDebug("answerRequest requestID " + req.getRequestID() + " from host "
					+ req.getClientAddress().getHostName() + " for blockID " + req.getBlockID() + " was a real read");
			// Get a list of all the requests that have requested this block ID
			ReentrantReadWriteLock requestListLock = mRequestLockMap.get(req.getBlockID());

			// Figure out if this is the first time the element has appeared
			// We need to know this because we need to know if we will be able to find this
			// element in the path or subtree
			boolean elementDoesExist = mPositionMap.getBlockPosition(req.getBlockID()) != -1;

			// Acquire write lock so we do not respond to all requests while accidentally
			// missing a soon to be sent fake read
			requestListLock.writeLock().lock();

			List<ClientRequest> requestList = mRequestMap.get(req.getBlockID());

			// Loop through each request in list of requests for this block
			while (!requestList.isEmpty()) {
				// Get current request that will be processed
				ClientRequest currentRequest = requestList.remove(0);
				TaoLogger.logDebug("answerRequest serving current requestID " + currentRequest.getRequestID()
						+ " from host " + currentRequest.getClientAddress().getHostName());
				responseMapEntry = mResponseMap.get(currentRequest);

				// Now we get the data from the desired block
				byte[] foundData;
				Tag foundTag;
				// First, from the subtree, find the bucket that has a block with blockID ==
				// req.getBlockID()
				if (elementDoesExist) {
					TaoLogger.logDebug("BlockID " + req.getBlockID() + " should exist somewhere");
					// The element should exist somewhere
					Block b = getDataFromBlock(currentRequest.getBlockID());
					foundData = b.getData();
					foundTag = b.getTag();
				} else {
					TaoLogger.logDebug("BlockID " + req.getBlockID() + " does not yet exist");
					// The element has never been created before
					foundData = new byte[TaoConfigs.BLOCK_SIZE];
					foundTag = new Tag();
				}

				// Check if the request was a write
				if (currentRequest.getType() == MessageTypes.CLIENT_WRITE_REQUEST) {
					TaoLogger.logForce("Processor is answering a write request. THIS SHOULDN'T HAPPEN");
					/*
					 * TaoLogger.logDebug("Write request BlockID " + req.getBlockID()); if
					 * (elementDoesExist) { // The element should exist somewhere
					 * writeDataToBlock(currentRequest.getBlockID(), currentRequest.getData(),
					 * currentRequest.getTag()); } else { Block newBlock =
					 * mPathCreator.createBlock(); newBlock.setBlockID(currentRequest.getBlockID());
					 * newBlock.setData(currentRequest.getData());
					 * newBlock.setTag(currentRequest.getTag());
					 * 
					 * // Add block to stash and assign random path position
					 * mStash.addBlock(newBlock); } canPutInPositionMap = true;
					 */
				} else {
					TaoLogger.logDebug("Read request BlockID " + req.getBlockID());
					// If the element doesn't exist, create an empty block
					// and add it to the stash
					if (!elementDoesExist) {
						Block newBlock = mPathCreator.createBlock();
						newBlock.setBlockID(currentRequest.getBlockID());
						newBlock.setData(new byte[TaoConfigs.BLOCK_SIZE]);
						newBlock.setTag(new Tag());

						// Add block to stash and assign random path position
						mStash.addBlock(newBlock);
					}
				}

				// Check if the server has responded to this request yet
				// NOTE: This is the part that answers all fake reads
				responseMapEntry.setData(foundData);
				responseMapEntry.setTag(foundTag);
				if (mResponseMap.get(currentRequest).getRetured()) {
					TaoLogger.logDebug("answerRequest requestID " + currentRequest.getRequestID() + " from host "
							+ currentRequest.getClientAddress().getHostName() + " is going to be responded to");

					mProfiler.answerRequestComplete(currentRequest);

					// Send the data to sequencer
					mSequencer.onReceiveResponse(currentRequest, resp, foundData, foundTag);

					// Remove this request from the response map
					mResponseMap.remove(currentRequest);
				}

				// After the first pass through the loop, the element is guaranteed to exist
				elementDoesExist = true;
			}

			// Release lock
			requestListLock.writeLock().unlock();

			// Assign block with blockID == req.getBlockID() to a new random path in
			// position map
			int newPathID = mCryptoUtil.getRandomPathID();
			TaoLogger.logInfo("Assigning blockID " + req.getBlockID() + " to path " + newPathID);
			mPositionMap.setBlockPosition(req.getBlockID(), newPathID);
		} else {
			TaoLogger.logInfo("answerRequest requestID " + req.getRequestID() + " from host "
					+ req.getClientAddress().getHostName() + " was a fake read, and real read has not responded yet");
		}

		// Now that the response has come back, remove one instance of the requested
		// block ID from mPathReqMultiSet
		// We have this here so that the path is not deleted while looking for block
		mPathReqMultiSet.remove(resp.getPathID());
	}

	/**
	 * @brief Method to get data from a block with the given blockID
	 * @param blockID
	 * @return the data from block
	 */
	public Block getDataFromBlock(long blockID) {
		TaoLogger.logDebug("Trying to get data for blockID " + blockID);
		TaoLogger.logDebug("I think this is at path: " + mPositionMap.getBlockPosition(blockID));

		// Due to multiple threads moving blocks around, we need to run this in a loop
		while (true) {
			// Check if the bucket containing this blockID is in the subtree
			Bucket targetBucket = mSubtree.getBucketWithBlock(blockID);

			if (targetBucket != null) {
				// If we found the bucket in the subtree, we can attempt to get the data from
				// the block in bucket
				TaoLogger.logInfo("Bucket containing block " + blockID + " found in subtree");
				Block data = targetBucket.getDataFromBlock(blockID);

				// Check if this data is not null
				if (data != null) {
					// If not null, we return the data
					TaoLogger.logDebug("Returning data for block " + blockID);
					return data;
				} else {
					// If null, we look again
					TaoLogger.logDebug("scuba But bucket does not have the data we want");

					// System.exit(1);
					// continue;
				}
			} else {
				// If the block wasn't in the subtree, it should be in the stash or orphan
				// blocks
				TaoLogger.logInfo("Cannot find block " + blockID + " in subtree");
				Block targetBlock = mStash.getBlock(blockID);
				if (targetBlock == null) {
					targetBlock = mOrphanBlocks.get(blockID);
				}

				if (targetBlock != null) {
					// If we found the block in the stash, return the data
					TaoLogger.logDebug("Returning data for block " + blockID);
					return targetBlock;
				} else {
					// If we did not find the block, we LOOK AGAIN
					TaoLogger.logDebug("scuba Cannot find in subtree or stash");
					// System.exit(1);
					// continue;
				}
			}
		}
	}

	public void writeDataToBlock(long blockID, byte[] data, Tag tag) {
		TaoLogger.logDebug("Trying to write data for blockID " + blockID);
		TaoLogger.logDebug("I think this is at path: " + mPositionMap.getBlockPosition(blockID));

		boolean elementDoesExist = mPositionMap.getBlockPosition(blockID) != -1;

		// Note: this is only reached when TaoInterface calls this method
		if (!elementDoesExist) {
			Block newBlock = mPathCreator.createBlock();
			newBlock.setBlockID(blockID);
			newBlock.setData(data);
			newBlock.setTag(tag);

			// Add block to stash and assign random path position
			mStash.addBlock(newBlock);

			int newPathID = mCryptoUtil.getRandomPathID();
			TaoLogger.logInfo("Assigning blockID " + blockID + " to path " + newPathID);
			mPositionMap.setBlockPosition(blockID, newPathID);
			return;
		}

		while (true) {
			// mSubtreeRWL.readLock().lock();
			// Check if block is in subtree
			Bucket targetBucket = mSubtree.getBucketWithBlock(blockID);
			if (targetBucket != null) {
				// If the bucket was found, we modify a block
				if (tag.compareTo(targetBucket.getDataFromBlock(blockID).getTag()) >= 0) {
					boolean success = targetBucket.modifyBlock(blockID, data, tag);
					if (!success) {
						TaoLogger.logForce("Found blockID " + blockID + " but failed to modify it.");
					}
				}
				// mSubtreeRWL.readLock().unlock();
				return;
			} else {
				// If we cannot find a bucket with the block, we check for the block in the
				// stash and orphan blocks
				Block targetBlock = mStash.getBlock(blockID);
				if (targetBlock == null) {
					targetBlock = mOrphanBlocks.get(blockID);
				}

				// If the block was found in the orphan blocks, we set the data for the block
				if (targetBlock != null) {
					if (tag.compareTo(targetBlock.getTag()) >= 0) {
						targetBlock.setData(data);
						targetBlock.setTag(tag);
					}
					// mSubtreeRWL.readLock().unlock();
					return;
				}
			}
			// mSubtreeRWL.readLock().unlock();
			TaoLogger.logWarning("Trying to look for blockID " + blockID + " again... This is bad.");
		}
	}

	@Override
	public void flush(long pathID) {
		mSubtreeRWL.writeLock().lock();

		TaoLogger.logInfo("Doing a flush for pathID " + pathID);

		// Get path that will be flushed
		Path pathToFlush = mSubtree.getPath(pathID);

		if (pathToFlush == null) {
			mSubtreeRWL.writeLock().unlock();
			return;
		}

		// Lock every bucket on the path
		// pathToFlush.lockPath();

		// Get a heap based on the block's path ID when compared to the target path ID
		PriorityQueue<Block> blockHeap = getHeap(pathID);

		// Clear path
		mSubtree.clearPath(pathID);

		// Variables to help with flushing path
		Block currentBlock;
		int level = TaoConfigs.TREE_HEIGHT;

		// Flush path
		while (!blockHeap.isEmpty() && level >= 0) {
			// Get block at top of heap
			currentBlock = blockHeap.peek();

			// Find the path ID that this block maps to
			long pid = mPositionMap.getBlockPosition(currentBlock.getBlockID());

			// Check if this block can be inserted at this level
			if (Utility.getGreatestCommonLevel(pathID, pid) >= level) {
				// If the block can be inserted at this level, get the bucket
				Bucket pathBucket = pathToFlush.getBucket(level);

				// Try to add this block into the path and update the bucket's timestamp
				if (pathBucket.addBlock(currentBlock, mTimestamp.get())) {
					// If we have successfully added the block to the bucket, we remove the block
					// from stash
					mStash.removeBlock(currentBlock);

					// Add new entry to subtree's map of block IDs to bucket
					mSubtree.mapBlockToBucket(currentBlock.getBlockID(), pathBucket);

					// If add was successful, remove block from heap and move on to next block
					// without decrementing the level we are adding to
					blockHeap.poll();
					continue;
				}
			}

			// If we are unable to add a block at this level, move on to next level
			level--;
		}

		// Unlock the path
		// pathToFlush.unlockPath();

		// Release subtree reader's lock
		mSubtreeRWL.writeLock().unlock();

		// Add remaining blocks in heap to stash
		if (!blockHeap.isEmpty()) {
			while (!blockHeap.isEmpty()) {
				mStash.addBlock(blockHeap.poll());
			}
		}

		// update our timestamp since we moved blocks around
		mTimestamp.getAndIncrement();
	}

	public void update_timestamp(long blockID) {
		// update the bucket timestamp if possible
		Bucket bucket = mSubtree.getBucketWithBlock(blockID);
		if (bucket != null) {
			bucket.setUpdateTime(mTimestamp.get());
		}

		// Add this path to the write queue
		long pathID = mPositionMap.getBlockPosition(blockID);
		if (pathID == -1) {
			TaoLogger.logForce(
					"Path ID for blockID " + blockID + " was unmapped during o_write. This should never happen!");
			System.exit(1);
		}
		synchronized (mWriteQueue) {
			TaoLogger.logInfo("Adding " + pathID + " to mWriteQueue");
			mWriteQueue.add(pathID);
		}
		mWriteBackCounter.getAndIncrement();

		mTimestamp.getAndIncrement();
	}

	/**
	 * @brief Method to create a max heap where the top element is the current block best suited to be placed along the path
	 * @param pathID
	 * @return max heap based on each block's path id when compared to the passed in pathID
	 */
	public PriorityQueue<Block> getHeap(long pathID) {
		// Get all the blocks from the stash and blocks from this path
		ArrayList<Block> blocksToFlush = new ArrayList<>();
		blocksToFlush.addAll(mStash.getAllBlocks());

		Bucket[] buckets = mSubtree.getPath(pathID).getBuckets();
		for (Bucket b : buckets) {
			blocksToFlush.addAll(b.getFilledBlocks());
		}

		// Remove duplicates
		Set<Block> hs = new HashSet<>();
		hs.addAll(blocksToFlush);
		blocksToFlush.clear();
		blocksToFlush.addAll(hs);
		blocksToFlush = Lists.newArrayList(Sets.newHashSet(blocksToFlush));

		// Create heap based on the block's path ID when compared to the target path ID
		PriorityQueue<Block> blockHeap = new PriorityQueue<>(TaoConfigs.BUCKET_SIZE,
				new BlockPathComparator(pathID, mPositionMap));
		blockHeap.addAll(blocksToFlush);
		return blockHeap;
	}

	@Override
	public void writeBack() {
		// Variable to keep track of the current mNextWriteBack
		long writeBackTime;

		// Check to see if a write back should be started
		if (mWriteBackCounter.get() >= mNextWriteBack) {
			// Multiple threads might pass first condition, must acquire lock in order to be
			// the thread that triggers
			// the write back
			if (mWriteBackLock.tryLock()) {
				// Theoretically could be rare condition when a thread acquires lock but another
				// thread has already
				// acquired the lock and incremented mNextWriteBack, so make sure that condition
				// still holds
				if (mWriteBackCounter.get() >= mNextWriteBack) {
					// Keep track of the time
					writeBackTime = mTimestamp.get();

					// Increment the next time we should write trigger write back
					mNextWriteBack += TaoConfigs.WRITE_BACK_THRESHOLD;

					// Unlock and continue with write back
					mWriteBackLock.unlock();
				} else {
					// Condition no longer holds, so unlock and return
					mWriteBackLock.unlock();
					return;
				}
			} else {
				// Another thread is going to execute write back for this current value of
				// mNextWriteBack, so return
				return;
			}
		} else {
			return;
		}

		// Make another variable for the write back time because Java says so
		long finalWriteBackTime = writeBackTime;

		mProfiler.writeBackStart(finalWriteBackTime);

		try {
			// Create a map that will map each InetSocketAddress to a list of paths that
			// will be written to it
			Map<InetSocketAddress, List<Long>> writebackMap = new HashMap<>();

			// Needed in order to clean up subtree later
			List<Long> allWriteBackIDs = new ArrayList<>();

			// Get the first TaoConfigs.WRITE_BACK_THRESHOLD path IDs from the mWriteQueue
			// and place them in the map
			for (int i = 0; i < TaoConfigs.WRITE_BACK_THRESHOLD; i++) {
				// Get a path ID
				Long currentID;
				synchronized (mWriteQueue) {
					currentID = mWriteQueue.remove();
				}
				TaoLogger.logInfo("Writeback for path id " + currentID + " with timestamp " + finalWriteBackTime);

				// Check what server is responsible for this path
				InetSocketAddress isa = mPositionMap.getServerForPosition(currentID);

				// Add this path ID to the map
				List<Long> temp = writebackMap.get(isa);
				if (temp == null) {
					temp = new ArrayList<>();
					writebackMap.put(isa, temp);
				}
				temp.add(currentID);
			}

			// Current storage server we are targeting (corresponds to index into list of
			// storage servers)
			int serverIndex = -1;

			// List of all the servers that successfully returned
			boolean[] serverDidReturn = new boolean[writebackMap.keySet().size()];

			// When a response is received from a server, this lock must be obtained to
			// modify serverDidReturn
			Object returnLock = new Object();

			// Deep copy of paths in subtree for writeback
			Map<InetSocketAddress, List<Path>> wbPaths = new HashMap<>();

			mSubtreeRWL.readLock().lock();

			// Make a deep copy of the needed paths from the subtree
			for (InetSocketAddress serverAddr : writebackMap.keySet()) {
				// Get the list of paths to be written for the current server
				List<Long> writebackPaths = writebackMap.get(serverAddr);

				List<Path> paths = new ArrayList<Path>();

				for (int i = 0; i < writebackPaths.size(); i++) {
					// Get path
					Path p = mSubtree.getPath(writebackPaths.get(i));
					if (p != null) {
						// Set the path to correspond to the relative leaf ID as present on the server
						// to be written to
						p.setPathID(mRelativeLeafMapper.get(p.getPathID()));
						TaoPath pathCopy = new TaoPath();
						pathCopy.initFromPath(p);

						/* Log which blocks are in the snapshot */
						for (Bucket bucket : pathCopy.getBuckets()) {
							for (Block b : bucket.getFilledBlocks()) {
								TaoLogger.logBlock(b.getBlockID(), "snapshot add");
							}
						}

						paths.add(pathCopy);
						allWriteBackIDs.add(writebackPaths.get(i));
					}
				}

				wbPaths.put(serverAddr, paths);
			}

			mSubtreeRWL.readLock().unlock();

			// Now we will send the writeback request to each server
			for (InetSocketAddress serverAddr : wbPaths.keySet()) {
				// Increment and save current server index
				serverIndex++;
				final int serverIndexFinal = serverIndex;

				// Get the list of paths to be written for the current server
				List<Path> paths = wbPaths.get(serverAddr);

				// Get all the encrypted path data
				byte[] dataToWrite = null;
				int pathSize = 0;

				TaoLogger.logInfo("Going to do writeback");
				for (int i = 0; i < paths.size(); i++) {
					// Get path
					Path p = paths.get(i);
					// If this is the first path, don't need to concat the data
					if (dataToWrite == null) {
						dataToWrite = mCryptoUtil.encryptPath(p);
						pathSize = dataToWrite.length;
					} else {
						dataToWrite = Bytes.concat(dataToWrite, mCryptoUtil.encryptPath(p));
					}
				}

				if (dataToWrite == null) {
					serverDidReturn[serverIndexFinal] = true;
					continue;
				}

				// Create the proxy write request
				ProxyRequest writebackRequest = mMessageCreator.createProxyRequest();
				writebackRequest.setType(MessageTypes.PROXY_WRITE_REQUEST);
				writebackRequest.setPathSize(pathSize);
				writebackRequest.setDataToWrite(dataToWrite);
				writebackRequest.setTimestamp(finalWriteBackTime);

				// Serialize the request
				byte[] encryptedWriteBackPaths = writebackRequest.serialize();

				// Create and run server
				Runnable writebackRunnable = () -> {
					try {
						TaoLogger.logDebug("Going to do writeback for server " + serverIndexFinal);

						mProfiler.writeBackPreSend(serverAddr, finalWriteBackTime);

						// Create channel and connect to server
						AsynchronousSocketChannel channel = AsynchronousSocketChannel.open(mThreadGroup);
						Future<Void> connection = channel.connect(serverAddr);
						connection.get();

						// First we send the message type to the server along with the size of the
						// message
						ByteBuffer messageType = MessageUtility.createMessageHeaderBuffer(
								MessageTypes.PROXY_WRITE_REQUEST, encryptedWriteBackPaths.length);
						Future<?> sendHeader;
						while (messageType.remaining() > 0) {
							sendHeader = channel.write(messageType);
							sendHeader.get();
						}
						messageType = null;

						// Send writeback paths
						ByteBuffer message = ByteBuffer.wrap(encryptedWriteBackPaths);
						Future<?> sendMessage;
						while (message.remaining() > 0) {
							sendMessage = channel.write(message);
							sendMessage.get();
						}
						message = null;
						TaoLogger.logDebug("Sent info, now waiting to listen for server " + serverIndexFinal);

						// Listen for server response
						ByteBuffer messageTypeAndSize = MessageUtility.createTypeReceiveBuffer();
						channel.read(messageTypeAndSize, null, new CompletionHandler<Integer, Void>() {
							@Override
							public void completed(Integer result, Void attachment) {
								// profiling
								mProfiler.writeBackPostRecv(serverAddr, finalWriteBackTime);

								// Flip the byte buffer for reading
								messageTypeAndSize.flip();

								// Parse the message type and size from server
								int[] typeAndLength = MessageUtility.parseTypeAndLength(messageTypeAndSize);
								int messageType = typeAndLength[0];
								int messageLength = typeAndLength[1];

								// Determine behavior based on response
								if (messageType == MessageTypes.SERVER_RESPONSE) {
									// Read the response
									ByteBuffer messageResponse = ByteBuffer.allocate(messageLength);
									channel.read(messageResponse, null, new CompletionHandler<Integer, Void>() {

										@Override
										public void completed(Integer result, Void attachment) {
											// Make sure we read the entire message
											while (messageResponse.remaining() > 0) {
												channel.read(messageResponse);
											}

											try {
												channel.close();
											} catch (IOException e) {
												e.printStackTrace(System.out);
											}

											// Flip byte buffer for reading
											messageResponse.flip();

											// Get data from response
											byte[] serialized = new byte[messageLength];
											messageResponse.get(serialized);

											// Create ServerResponse based on data
											ServerResponse response = mMessageCreator.createServerResponse();
											response.initFromSerialized(serialized);

											long serverProcessingTime = response.getProcessingTime();
											mProfiler.writeBackServerProcessingTime(serverAddr, finalWriteBackTime,
													serverProcessingTime);

											// Check to see if the write succeeded or not
											if (response.getWriteStatus()) {
												// Acquire return lock
												synchronized (returnLock) {
													// Set that this server did return
													serverDidReturn[serverIndexFinal] = true;

													// Check if all the servers have returned
													boolean allReturn = true;
													for (int n = 0; n < serverDidReturn.length; n++) {
														if (!serverDidReturn[n]) {
															allReturn = false;
															break;
														}
													}

													// If all the servers have successfully responded, we can delete
													// nodes from subtree
													if (allReturn) {
														mProfiler.writeBackComplete(finalWriteBackTime);
														Set<Long> set = new HashSet<>();
														for (Long l : mPathReqMultiSet.elementSet()) {
															set.add(l);
														}

														// Iterate through every path that was written, check if there
														// are any nodes
														// we can delete
														mSubtreeRWL.writeLock().lock();
														for (Long pathID : allWriteBackIDs) {
															// Upon response, delete all nodes in subtree whose
															// timestamp is <= timeStamp, and are not in
															// mPathReqMultiSet
															mSubtree.deleteNodes(pathID, finalWriteBackTime, set);
														}
														mSubtreeRWL.writeLock().unlock();
														logNumBlocks();
													}
												}
											}
										}

										@Override
										public void failed(Throwable exc, Void attachment) {
											TaoLogger.logForce(
													"Failed to read the rest of the server response after writing back");
											exc.printStackTrace(System.out);
										}
									});
								} else {
									TaoLogger.logForce("Got unexpected message type from the server");
								}
							}

							@Override
							public void failed(Throwable exc, Void attachment) {
								TaoLogger.logForce("Failed to read server response header after writing back");
								exc.printStackTrace(System.out);
							}
						});
					} catch (Exception e) {
						e.printStackTrace(System.out);
					}
				};
				mProcessorThreadPool.execute(writebackRunnable);
			}
		} catch (Exception e) {
			e.printStackTrace(System.out);
		}
	}

	public void disconnectClient(AsynchronousSocketChannel channel) {
		if (mProxyToServerChannelMap.containsKey(channel)) {
			TaoLogger.logInfo("Removing client channels to server for client " + channel.toString());
			for (Map.Entry<InetSocketAddress, AsynchronousSocketChannel> entry : mProxyToServerChannelMap.get(channel)
					.entrySet()) {
				try {
					entry.getValue().close();
				} catch (IOException e) {
					e.printStackTrace(System.out);
				}
			}
			mProxyToServerChannelMap.remove(channel);
			mAsyncProxyToServerSemaphoreMap.remove(channel);
		} else {
			TaoLogger.logInfo("Client " + channel.toString() + " wasn't found in channel map");
			TaoLogger.logInfo(mProxyToServerChannelMap.keySet().toString());
		}
	}

	public void initLoadTest() {
		mDoingLoadTest = true;
		mBucketedSubtreeBlocks.clear();
		mBucketedStashBlocks.clear();
		mBucketedOrphanBlocks.clear();
		mLoadTestStartTime = System.currentTimeMillis();
	}

	public void finishLoadTest() {
		mDoingLoadTest = false;

		TaoLogger.logForce("Number of Subtree Blocks over Time:");
		StringBuilder numSubtreeBlocksBuilder = new StringBuilder();
		for (int i = 0; i < mBucketedSubtreeBlocks.size(); i++) {
			numSubtreeBlocksBuilder.append(String.format("(%d,%f)", i * NUM_BLOCKS_SAMPLE_INTERVAL / 1000,
					mBucketedSubtreeBlocks.get(i).stream().mapToDouble(a -> a).average().orElse(0)));
		}
		TaoLogger.logForce(numSubtreeBlocksBuilder.toString());

		TaoLogger.logForce("Number of Stash Blocks over Time:");
		StringBuilder numStashBlocksBuilder = new StringBuilder();
		for (int i = 0; i < mBucketedStashBlocks.size(); i++) {
			numStashBlocksBuilder.append(String.format("(%d,%f)", i * NUM_BLOCKS_SAMPLE_INTERVAL / 1000,
					mBucketedStashBlocks.get(i).stream().mapToDouble(a -> a).average().orElse(0)));
		}
		TaoLogger.logForce(numStashBlocksBuilder.toString());

		TaoLogger.logForce("Number of Orphan Blocks over Time:");
		StringBuilder numOrphanBlocksBuilder = new StringBuilder();
		for (int i = 0; i < mBucketedOrphanBlocks.size(); i++) {
			numOrphanBlocksBuilder.append(String.format("(%d,%f)", i * NUM_BLOCKS_SAMPLE_INTERVAL / 1000,
					mBucketedOrphanBlocks.get(i).stream().mapToDouble(a -> a).average().orElse(0)));
		}
		TaoLogger.logForce(numOrphanBlocksBuilder.toString());

		TaoLogger.logForce(mProfiler.getProxyStatistics());

		System.gc();
	}

}

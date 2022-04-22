package TaoClient;

import Configuration.ArgumentParser;
import Configuration.TaoConfigs;
import Messages.*;
import TaoProxy.*;

import com.google.common.math.Quantiles;
import com.google.common.primitives.Bytes;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.Channel;
import java.nio.channels.CompletionHandler;
import java.nio.BufferUnderflowException;
import java.security.SecureRandom;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.math3.distribution.ZipfDistribution;

/**
 * @brief Class to represent a client of TaoStore
 */
public class TaoClient implements Client {
	// The address of the proxy
	protected List<InetSocketAddress> mProxyAddresses;

	// The address of this client
	protected InetSocketAddress mClientAddress;

	// A MessageCreator to create different types of messages to be passed from
	// client, proxy, and server
	protected MessageCreator mMessageCreator;

	// Counter to keep track of current request number
	// Incremented after each request
	protected AtomicLong mRequestID;

	// Thread group for asynchronous sockets
	protected AsynchronousChannelGroup mThreadGroup;

	// Map of request IDs to ProxyResponses. Used to differentiate which request a
	// response is answering
	protected Map<Long, ProxyResponse> mResponseWaitMap;

	// Channel to proxy
	protected Map<Integer, AsynchronousSocketChannel> mChannels;

	// ExecutorService for async reads/writes
	protected ExecutorService mExecutor;

	// Next operation ID to use
	protected OperationID mNextOpID;

	/* Below static variables are used for load testing */

	public static Short sNextClientID = 0;

	// Used for measuring latency when running load test
	public static List<Long> sResponseTimes = new ArrayList<>();

	// response times bucketed by time interval
	public static ArrayList<ArrayList<Long>> sBucketedResponseTimes = new ArrayList<ArrayList<Long>>();

	// Used for locking the async load test until all the operations are replied to
	public static Object sAsycLoadLock = new Object();

	// List of bytes used for writing blocks as well as comparing the results of
	// returned block data
	public static ArrayList<byte[]> sListOfBytes = new ArrayList<>();

	// Number of concurrent clients in load test
	public static int CONCURRENT_CLIENTS = 1;

	// Load test length in ms
	public static int LOAD_TEST_LENGTH = 1000 * 2 * 60;

	// The number of warmup operations to perform before beginning the load test
	public static int WARMUP_OPERATIONS = 100;

	// time interval for bucketting throughputs and response times
	public static int SAMPLE_INTERVAL = 10 * 1000;

	public static final int TIMEOUT_DELAY = 2000;

	public static ArrayList<Double> sThroughputs = new ArrayList<>();

	// throughput bucketed by time interval
	public static ArrayList<Long> sBucketedThroughputs = new ArrayList<Long>();

	public static long loadTestStartTime;

	public short mClientID;

	public static Profiler sProfiler = new TaoProfiler(-1);

	public Map<Integer, Long> mBackoffTimeMap = new HashMap<>();
	public Map<Integer, Integer> mBackoffCountMap = new HashMap<>();
	public ReadWriteLock backoffLock = new ReentrantReadWriteLock();

	public static boolean randomQuorum;
	// how many of the most recent latencies we should keep around to get the median
	public static final int NUM_LATENCIES = 1000;
	// keeps a list of the most recent NUM_LATENCIES for each site (or replica)
	public Map<Integer, Deque<Long>> mSiteLatencies = new HashMap<Integer, Deque<Long>>();
	public ReadWriteLock mSiteLatenciesLock = new ReentrantReadWriteLock();

	public TaoClient(short id) {
		try {
			// Initialize needed constants
			TaoConfigs.initConfiguration();

			TaoLogger.logLevel = TaoLogger.LOG_OFF;

			TaoLogger.logInfo("making client");

			if (id == -1) {
				synchronized (sNextClientID) {
					id = sNextClientID;
					sNextClientID++;
				}
			}
			mClientID = id;
			mNextOpID = new OperationID();
			mNextOpID.seqNum = 0;
			mNextOpID.clientID = id;

			// Get the current client's IP
			String currentIP = InetAddress.getLocalHost().getHostAddress();
			mClientAddress = new InetSocketAddress(currentIP, TaoConfigs.CLIENT_PORT);

			// Create message creator
			mMessageCreator = new TaoMessageCreator();

			// Initialize response wait map
			mResponseWaitMap = new ConcurrentHashMap<>();

			// Thread group used for asynchronous I/O
			mThreadGroup = AsynchronousChannelGroup.withFixedThreadPool(TaoConfigs.PROXY_THREAD_COUNT,
					Executors.defaultThreadFactory());

			// Create executor
			mExecutor = Executors.newFixedThreadPool(TaoConfigs.PROXY_THREAD_COUNT, Executors.defaultThreadFactory());

			// Request ID counter
			mRequestID = new AtomicLong();

			initializeConnections();

			Runnable serializeProcedure = () -> processAllProxyReplies();
			new Thread(serializeProcedure).start();
			TaoLogger.logInfo("made client");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	protected void initializeConnections() {
		// Initialize list of proxy addresses
		mProxyAddresses = new ArrayList<InetSocketAddress>();
		for (int i = 0; i < TaoConfigs.ORAM_UNITS.size(); i++) {
			mProxyAddresses.add(new InetSocketAddress(TaoConfigs.ORAM_UNITS.get(i).proxyHost,
					TaoConfigs.ORAM_UNITS.get(i).proxyPort));
			mBackoffTimeMap.put(i, Long.valueOf(0));
			mBackoffCountMap.put(i, 0);
		}

		boolean connected = false;
		mChannels = new HashMap<Integer, AsynchronousSocketChannel>();
		while (!connected) {
			try {
				// Create and connect channels to proxies
				for (int i = 0; i < TaoConfigs.ORAM_UNITS.size(); i++) {
					AsynchronousSocketChannel channel = AsynchronousSocketChannel.open(mThreadGroup);
					Future<?> connection = channel.connect(mProxyAddresses.get(i));
					connection.get();
					mChannels.put(i, channel);
				}
				connected = true;
			} catch (Exception e) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e3) {
				}
			}
		}
	}

	protected void finalize() {
		try {
			for (Channel channel : mChannels.values()) {
				channel.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public byte[] read(long blockID, int unitID) {
		try {
			// Make request
			ClientRequest request = makeRequest(MessageTypes.CLIENT_READ_REQUEST, blockID, null, null, null, null);

			// Send read request
			ProxyResponse response = sendRequestWait(request, unitID);

			// Return read data
			return response.getReturnData();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

	@Override
	public boolean write(long blockID, byte[] data, int unitID) {
		try {
			// Make request
			ClientRequest request = makeRequest(MessageTypes.CLIENT_WRITE_REQUEST, blockID, data, null, null, null);

			// Send write request
			ProxyResponse response = sendRequestWait(request, unitID);

			// Return write status
			return response.getWriteStatus();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	private void processAllProxyReplies() {
		Iterator<Entry<Integer, AsynchronousSocketChannel>> it = mChannels.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<Integer, AsynchronousSocketChannel> entry = (Entry<Integer, AsynchronousSocketChannel>) it.next();
			TaoLogger.logInfo("Setting up thread for process " + entry.getKey());
			Runnable serializeProcedure = () -> processProxyReplies(entry.getKey());
			new Thread(serializeProcedure).start();
		}
	}

	private void processProxyReplies(int unitID) {
		ByteBuffer typeByteBuffer = MessageUtility.createTypeReceiveBuffer();

		AsynchronousSocketChannel channel = mChannels.get(unitID);
		// Asynchronously read message
		channel.read(typeByteBuffer, null, new CompletionHandler<Integer, Void>() {
			@Override
			public void completed(Integer result, Void attachment) {
				// Flip the byte buffer for reading
				typeByteBuffer.flip();

				// Figure out the type of the message
				int[] typeAndLength;
				try {
					typeAndLength = MessageUtility.parseTypeAndLength(typeByteBuffer);
				} catch (BufferUnderflowException e) {
					TaoLogger.logWarning("Unit " + unitID + " is down");
					try {
						Thread.sleep(10000);
					} catch (InterruptedException interruptError) {
					}
					AsynchronousSocketChannel newChannel;

					TaoLogger.logWarning("Trying to reconnect to unit " + unitID);
					boolean connected = false;
					while (!connected) {
						try {
							channel.close();
							newChannel = AsynchronousSocketChannel.open(mThreadGroup);
							Future<?> connection = newChannel.connect(mProxyAddresses.get(unitID));
							connection.get();
							TaoLogger.logWarning("Successfully reconnected to unit " + unitID);
							mChannels.put(unitID, newChannel);
							processProxyReplies(unitID);
							return;
						} catch (Exception connError) {
						}
					}
					// TODO: restore connection and recurse
					return;
				}
				int messageType = typeAndLength[0];
				int messageLength = typeAndLength[1];

				// Serve message based on type
				if (messageType == MessageTypes.PROXY_RESPONSE) {
					// Get the rest of the message
					ByteBuffer messageByteBuffer = ByteBuffer.allocate(messageLength);

					// Do one last asynchronous read to get the rest of the message
					channel.read(messageByteBuffer, null, new CompletionHandler<Integer, Void>() {
						@Override
						public void completed(Integer result, Void attachment) {
							// Make sure we read all the bytes
							while (messageByteBuffer.remaining() > 0) {
								channel.read(messageByteBuffer, null, this);
								return;
							}
							// Flip the byte buffer for reading
							messageByteBuffer.flip();

							// Get the rest of the bytes for the message
							byte[] requestBytes = new byte[messageLength];
							messageByteBuffer.get(requestBytes);

							// Initialize ProxyResponse object based on read bytes
							ProxyResponse proxyResponse = mMessageCreator.createProxyResponse();
							proxyResponse.initFromSerialized(requestBytes);

							// Get the ProxyResponse from map and initialize it
							ProxyResponse clientAnswer = mResponseWaitMap.get(proxyResponse.getClientRequestID());
							clientAnswer.initFromSerialized(requestBytes);

							sProfiler.clientRequestPostRecv(proxyResponse.getClientRequestID(),
									proxyResponse.getWriteStatus(), unitID);
							sProfiler.proxyProcessingTime(proxyResponse.getClientRequestID(),
									proxyResponse.getWriteStatus(), unitID, proxyResponse.getProcessingTime());

							// Notify thread waiting for this response id
							synchronized (clientAnswer) {
								clientAnswer.notifyAll();
								mResponseWaitMap.remove(clientAnswer.getClientRequestID());

								processProxyReplies(unitID);
							}
						}

						@Override
						public void failed(Throwable exc, Void attachment) {
							exc.printStackTrace();
						}
					});
				}
			}

			@Override
			public void failed(Throwable exc, Void attachment) {
				exc.printStackTrace();
			}
		});
	}

	public int selectUnit(Set<Integer> quorum) {
		// Find the ORAM units which can be contacted at this time
		ArrayList<Integer> available = new ArrayList<>();

		while (available.isEmpty()) {
			backoffLock.readLock().lock();

			Long time = System.currentTimeMillis();
			Long soonestBackoffTime = Long.valueOf(-1);
			for (int i = 0; i < TaoConfigs.ORAM_UNITS.size(); i++) {
				// Ignore any units already in the quorum
				if (quorum.contains(i)) {
					continue;
				}

				Long timeout = mBackoffTimeMap.get(i);
				if (time > timeout) {
					available.add(i);
				} else if (soonestBackoffTime == -1 || timeout < soonestBackoffTime) {
					soonestBackoffTime = timeout;
				}
			}

			backoffLock.readLock().unlock();

			// If none of the timeouts have expired yet, wait for the soonest
			// one to expire
			if (available.isEmpty()) {
				try {
					Thread.sleep(soonestBackoffTime - time);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		if (!randomQuorum) {
			// pick the available site with the lowest latency by averaging the latency
			mSiteLatenciesLock.readLock().lock();
			Optional<Entry<Integer, Deque<Long>>> site_entry = mSiteLatencies.entrySet().stream()
					.filter(entry -> available.contains(entry.getKey()) && !entry.getValue().isEmpty())
					.min(Comparator.comparingDouble(entry -> Quantiles.median().compute(entry.getValue())));
			mSiteLatenciesLock.readLock().unlock();
			// it's possible that we don't have latencies for any of the available sites
			if (site_entry.isPresent()) {
				if (TaoLogger.logLevel == TaoLogger.LOG_INFO) {
					// want to avoid recomputing the median if logging is off
					TaoLogger.logInfo("Selected unit " + site_entry.get().getKey() + " with median latency: "
							+ Quantiles.median().compute(site_entry.get().getValue()) + " ms");
				}
				return site_entry.get().getKey();
			}
		}
		// otherwise pick a random site from the available ones to add to the quorum
		return available.get((new Random()).nextInt(available.size()));
	}

	public Set<Integer> buildQuorum() {
		Set<Integer> quorum = new LinkedHashSet<>();

		while (quorum.size() < (int) ((TaoConfigs.ORAM_UNITS.size() + 1) / 2)) {
			quorum.add(selectUnit(quorum));
		}

		return quorum;
	}

	public void markResponsive(int unitID) {
		backoffLock.writeLock().lock();

		mBackoffCountMap.put(unitID, 0);

		backoffLock.writeLock().unlock();
	}

	public void markUnresponsive(int unitID) {
		backoffLock.writeLock().lock();
		long time = System.currentTimeMillis();
		mBackoffCountMap.put(unitID, mBackoffCountMap.get(unitID) + 1);

		// Do exponential backoff
		long backoff = (long) Math.pow(2, mBackoffCountMap.get(unitID)) + 500 + (new Random().nextInt(500));
		TaoLogger.logWarning("Unit " + unitID + " will not be contacted for " + backoff + "ms");
		mBackoffTimeMap.put(unitID, time + backoff);
		backoffLock.writeLock().unlock();
	}

	public byte[] logicalOperation(long blockID, byte[] data, boolean isWrite) {
		TaoLogger.logInfo("\n\n");

		TaoLogger.logInfo("starting logical op");
		// Assign the operation a unique ID
		OperationID opID;
		synchronized (mNextOpID) {
			opID = mNextOpID;
			mNextOpID = mNextOpID.getNext();
		}

		// Select the initial quorum for this operation
		TaoLogger.logInfo("Quorum contains:");
		Set<Integer> quorum = buildQuorum();
		for (int i : quorum) {
			TaoLogger.logInfo(Integer.toString(i));
		}

		TaoLogger.logInfo("Starting logical operation " + opID);

		// unique opID across all clients
		sProfiler.readQuorumPreSend(opID);

		// Broadcast read(blockID) to all ORAM units
		Map<Integer, Long> timeStart = new HashMap<>();
		Map<Integer, Future<ProxyResponse>> readResponsesWaiting = new HashMap<Integer, Future<ProxyResponse>>();
		Map<Integer, Future<ProxyResponse>> writeResponsesWaiting = new HashMap<Integer, Future<ProxyResponse>>();
		for (int i : quorum) {
			readResponsesWaiting.put(i, readAsync(blockID, i, opID));
			timeStart.put(i, System.currentTimeMillis());
		}

		byte[] writebackVal = null;
		Tag tag = null;

		// Wait for a read quorum (here a simple majority) of responses
		HashSet<Integer> readResponses = new HashSet<Integer>();
		HashSet<Integer> writeResponses = new HashSet<Integer>();
		boolean firstWrite = true;
		while (writeResponses.size() < (int) ((TaoConfigs.ORAM_UNITS.size() + 1) / 2)) {
			if (!firstWrite) {
				TaoLogger.logForce("First write failed, retrying...");
			}
			while (readResponses.size() < (int) ((TaoConfigs.ORAM_UNITS.size() + 1) / 2)) {
				// refill the quorum
				while (quorum.size() < (int) ((TaoConfigs.ORAM_UNITS.size() + 1) / 2)) {
					TaoLogger.logInfo("Adding unit to quorum");
					int addedUnit = selectUnit(quorum);
					quorum.add(addedUnit);
					readResponsesWaiting.put(addedUnit, readAsync(blockID, addedUnit, opID));
					timeStart.put(addedUnit, System.currentTimeMillis());
				}

				Iterator<Entry<Integer, Future<ProxyResponse>>> it = readResponsesWaiting.entrySet().iterator();
				while (it.hasNext()) {
					Map.Entry<Integer, Future<ProxyResponse>> entry = (Entry<Integer, Future<ProxyResponse>>) it.next();
					if (entry.getValue().isDone()) {
						try {
							TaoLogger.logInfo("From proxy " + entry.getKey() + ": got value "
									+ entry.getValue().get().getReturnData()[0] + " with tag "
									+ entry.getValue().get().getReturnTag());

							if (!randomQuorum) {
								// record the read latency
								mSiteLatenciesLock.writeLock().lock();
								mSiteLatencies.putIfAbsent(entry.getKey(), new ArrayDeque<Long>());
								Deque<Long> siteLatencies = mSiteLatencies.get(entry.getKey());
								if (siteLatencies.size() == NUM_LATENCIES) {
									siteLatencies.removeFirst();
								}
								siteLatencies.addLast(System.currentTimeMillis() - timeStart.get(entry.getKey()));
								mSiteLatenciesLock.writeLock().unlock();
							}

							byte[] val = entry.getValue().get().getReturnData();
							Tag responseTag = entry.getValue().get().getReturnTag();

							// Set writeback value to the value with the greatest tag
							if (tag == null || responseTag.compareTo(tag) >= 0) {
								writebackVal = val;
								tag = responseTag;
							}

							it.remove();
							readResponses.add(entry.getKey());

							markResponsive(entry.getKey());
						} catch (Exception e) {
							TaoLogger.logForce(e.toString());
							e.printStackTrace();
						}
					} else if (System.currentTimeMillis() > timeStart.get(entry.getKey()) + TIMEOUT_DELAY) {
						entry.getValue().cancel(true);
						TaoLogger.logInfo("Timed out during read waiting for proxy " + entry.getKey());
						it.remove();

						// Remove unit from quorum and mark unresponsive
						quorum.remove(entry.getKey());
						markUnresponsive(entry.getKey());
					}
				}
			}

			if (firstWrite) {
				// we are not profiling the case where we have to try again after the read
				// succeeds but the write fails
				sProfiler.readQuorumPostRecv(opID);
			}

			// Cancel all pending reads, so that threads are not wasted
			Iterator<Entry<Integer, Future<ProxyResponse>>> it = readResponsesWaiting.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<Integer, Future<ProxyResponse>> entry = (Entry<Integer, Future<ProxyResponse>>) it.next();
				entry.getValue().cancel(true);
				it.remove();
			}

			// If write, set writeback value to client's value
			// and increment tag
			if (isWrite && firstWrite) {
				writebackVal = data;
				tag.seqNum = tag.seqNum + 1;
				tag.clientID = mClientID;
			}

			sProfiler.writeQuorumPreSend(opID);

			// write to quorum members that we haven't acknowledged a response from
			for (int i : quorum) {
				if (!writeResponses.contains(i)) {
					writeResponsesWaiting.put(i, writeAsync(blockID, writebackVal, tag, i, opID));
					timeStart.put(i, System.currentTimeMillis());
				}
			}

			// Wait for a write quorum of acks (here a simple majority)
			while (!writeResponsesWaiting.isEmpty()) {
				it = writeResponsesWaiting.entrySet().iterator();
				while (it.hasNext()) {
					Map.Entry<Integer, Future<ProxyResponse>> entry = (Map.Entry<Integer, Future<ProxyResponse>>) it
							.next();
					if (entry.getValue().isDone()) {
						try {
							TaoLogger.logInfo("Got ack from proxy " + entry.getKey());

							if (!randomQuorum) {
								// record the write latency
								mSiteLatenciesLock.writeLock().lock();
								mSiteLatencies.putIfAbsent(entry.getKey(), new ArrayDeque<Long>());
								Deque<Long> siteLatencies = mSiteLatencies.get(entry.getKey());
								if (siteLatencies.size() == NUM_LATENCIES) {
									siteLatencies.removeFirst();
								}
								siteLatencies.addLast(System.currentTimeMillis() - timeStart.get(entry.getKey()));
								mSiteLatenciesLock.writeLock().unlock();
							}

							it.remove();
							writeResponses.add(entry.getKey());
							markResponsive(entry.getKey());

							if (entry.getValue().get().getFailed()) {
								// we got a negative ack in the write p hase so abort the operation
								// cancel waiting on responses so threa ds aren't wasted
								while (it.hasNext()) {
									Map.Entry<Integer, Future<ProxyResponse>> entry2 = (Entry<Integer, Future<ProxyResponse>>) it
											.next();
									entry2.getValue().cancel(true);
									it.remove();
								}
								return null;
							}
						} catch (Exception e) {
							TaoLogger.logInfo(e.toString());
							e.printStackTrace();
						}
					} else if (System.currentTimeMillis() > timeStart.get(entry.getKey()) + TIMEOUT_DELAY) {
						entry.getValue().cancel(true);
						TaoLogger.logInfo("Timed out during write waiting for proxy " + entry.getKey());

						it.remove();

						// Remove unit from quorum and mark unresponsive
						quorum.remove(entry.getKey());
						// remove from the read responses so that we read from someone else
						readResponses.remove(entry.getKey());
						markUnresponsive(entry.getKey());
					}
				}
			}
			firstWrite = false;
		}

		sProfiler.writeQuorumPostRecv(opID);

		// Cancel all pending writes, so that threads are not wasted
		Iterator<Entry<Integer, Future<ProxyResponse>>> it = writeResponsesWaiting.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<Integer, Future<ProxyResponse>> entry = (Map.Entry<Integer, Future<ProxyResponse>>) it.next();
			entry.getValue().cancel(true);
			it.remove();
		}

		TaoLogger.logInfo("\n\n");

		return writebackVal;
	}

	/**
	 * @brief Method to make a client request
	 * @param type
	 * @param blockID
	 * @param data
	 * @param extras
	 * @return a client request
	 */
	protected ClientRequest makeRequest(int type, long blockID, byte[] data, Tag tag, OperationID opID,
			List<Object> extras) {
		// Keep track of requestID and increment it
		// Because request IDs must be globally unique, we bit-shift the
		// request ID to the left and add the client ID, thereby
		// ensuring that no two clients use the same request ID
		long requestID = (Long.highestOneBit(TaoConfigs.MAX_CLIENT_ID + 1) << 1) * mRequestID.getAndAdd(1) + mClientID;

		// Create client request
		ClientRequest request = mMessageCreator.createClientRequest();
		request.setBlockID(blockID);
		request.setRequestID(requestID);
		request.setClientAddress(mClientAddress);

		if (opID == null) {
			opID = new OperationID();
		}
		request.setOpID(opID);
		if (tag == null) {
			tag = new Tag();
		}
		request.setTag(tag);

		// Set additional data depending on message type
		if (type == MessageTypes.CLIENT_READ_REQUEST) {
			request.setData(new byte[TaoConfigs.BLOCK_SIZE]);
		} else if (type == MessageTypes.CLIENT_WRITE_REQUEST) {
			request.setData(data);
		}

		request.setType(type);

		return request;
	}

	/**
	 * @brief Private helper method to send request to proxy and wait for response
	 * @param request
	 * @return ProxyResponse to request
	 */
	protected ProxyResponse sendRequestWait(ClientRequest request, int unitID) {
		try {
			// Create an empty response and put it in the mResponseWaitMap
			ProxyResponse proxyResponse = mMessageCreator.createProxyResponse();
			mResponseWaitMap.put(request.getRequestID(), proxyResponse);

			sProfiler.clientRequestPreSend(request.getRequestID(),
					request.getType() == MessageTypes.CLIENT_WRITE_REQUEST, unitID);

			// Send request and wait until response
			synchronized (proxyResponse) {
				sendRequestToProxy(request, unitID);
				proxyResponse.wait();
			}

			// Return response
			return proxyResponse;
		} catch (Exception e) {
		}

		return null;
	}

	/**
	 * @brief Private helper method to send a read or write request to a TaoStore
	 *        proxy
	 * @param request
	 * @return a ProxyResponse
	 */
	protected void sendRequestToProxy(ClientRequest request, int unitID) {
		try {
			// Send request to proxy
			byte[] serializedRequest = request.serialize();
			byte[] requestHeader = MessageUtility.createMessageHeaderBytes(request.getType(), serializedRequest.length);
			ByteBuffer requestMessage = ByteBuffer.wrap(Bytes.concat(requestHeader, serializedRequest));

			// Send message to proxy
			synchronized (mChannels.get(unitID)) {
				TaoLogger.logDebug("Sending request #" + request.getRequestID());
				while (requestMessage.remaining() > 0) {
					Future<Integer> writeResult = mChannels.get(unitID).write(requestMessage);
					try {
						writeResult.get();
					} catch (Exception e) {
						TaoLogger.logWarning("Unable to contact unit " + unitID);
						return;
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public Future<ProxyResponse> readAsync(long blockID, int unitID, OperationID opID) {
		Callable<ProxyResponse> readTask = () -> {
			// Make request
			ClientRequest request = makeRequest(MessageTypes.CLIENT_READ_REQUEST, blockID, null, null, opID, null);

			// Send read request
			ProxyResponse response = sendRequestWait(request, unitID);
			return response;
		};

		Future<ProxyResponse> future = mExecutor.submit(readTask);

		return future;
	}

	@Override
	public Future<ProxyResponse> writeAsync(long blockID, byte[] data, Tag tag, int unitID, OperationID opID) {
		Callable<ProxyResponse> readTask = () -> {
			// Make request
			ClientRequest request = makeRequest(MessageTypes.CLIENT_WRITE_REQUEST, blockID, data, tag, opID, null);

			// Send write request
			ProxyResponse response = sendRequestWait(request, unitID);
			return response;
		};

		Future<ProxyResponse> future = mExecutor.submit(readTask);

		return future;
	}

	private void sendMessageToProxy(ClientRequest request, InetSocketAddress proxyAddress) {
		try {
			// Get proxy name and port
			Socket clientSocket = new Socket(proxyAddress.getHostName(), proxyAddress.getPort());

			// Create output stream
			DataOutputStream output = new DataOutputStream(clientSocket.getOutputStream());

			byte[] serializedRequest = request.serialize();
			byte[] header = MessageUtility.createMessageHeaderBytes(request.getType(), serializedRequest.length);
			output.write(header);

			// Close streams and ports
			clientSocket.close();
			output.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void printSubtree() {
		ClientRequest request = makeRequest(MessageTypes.PRINT_SUBTREE, 0, null, null, null, null);
		sendMessageToProxy(request, mProxyAddresses.get(0));
	}

	public void initProxyLoadTest() {
		ClientRequest request = makeRequest(MessageTypes.INIT_LOAD_TEST, 0, null, null, null, null);
		for (InetSocketAddress proxyAddress : mProxyAddresses) {
			sendMessageToProxy(request, proxyAddress);
		}
	}

	public void finishProxyLoadTest() {
		ClientRequest request = makeRequest(MessageTypes.FINISH_LOAD_TEST, 0, null, null, null, null);
		for (InetSocketAddress proxyAddress : mProxyAddresses) {
			sendMessageToProxy(request, proxyAddress);
		}
	}

	@Override
	public void writeStatistics(int unitID) {
		try {
			// Get proxy name and port
			Socket clientSocket = new Socket(mProxyAddresses.get(unitID).getHostName(),
					mProxyAddresses.get(unitID).getPort());

			// Create output stream
			DataOutputStream output = new DataOutputStream(clientSocket.getOutputStream());

			// Create client request
			ClientRequest request = makeRequest(MessageTypes.WRITE_STATS, -1, null, null, null, null);

			byte[] serializedRequest = request.serialize();
			byte[] header = MessageUtility.createMessageHeaderBytes(request.getType(), serializedRequest.length);
			output.write(header);

			// Close streams and ports
			clientSocket.close();
			output.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static boolean doLoadTestOperation(Client client, int readOrWrite, long targetBlock) {
		long start;
		boolean success = true;
		if (readOrWrite == 0) {
			TaoLogger.logInfo("Doing read request #" + ((TaoClient) client).mRequestID.get());

			// Send read and keep track of response time
			start = System.currentTimeMillis();

			client.logicalOperation(targetBlock, null, false);
		} else {
			TaoLogger.logInfo("Doing write request #" + ((TaoClient) client).mRequestID.get());

			// Send write and keep track of response time
			byte[] dataToWrite = new byte[TaoConfigs.BLOCK_SIZE];
			Arrays.fill(dataToWrite, (byte) targetBlock);

			start = System.currentTimeMillis();

			boolean writeStatus = (client.logicalOperation(targetBlock, dataToWrite, true) != null);

			if (!writeStatus) {
				TaoLogger.logForce("Write failed for block " + targetBlock);
				success = false;
				System.exit(1);
			}
		}
		synchronized (sResponseTimes) {
			sResponseTimes.add(System.currentTimeMillis() - start);
			while (sBucketedResponseTimes
					.size() < (int) (1 + (System.currentTimeMillis() - loadTestStartTime) / SAMPLE_INTERVAL)) {
				sBucketedResponseTimes.add(new ArrayList<Long>());
			}
			sBucketedResponseTimes.get((int) ((System.currentTimeMillis() - loadTestStartTime) / SAMPLE_INTERVAL))
					.add(System.currentTimeMillis() - start);
		}

		return success;
	}

	public static void loadTest(int concurrentClients, int loadTestLength, int warmupOperations, double rwRatio,
			double zipfExp, int clientID) throws InterruptedException {
		// Warm up the system
		TaoClient warmUpClient = new TaoClient((short) -1);
		SecureRandom sr = new SecureRandom();
		long totalNodes = (long) Math.pow(2, TaoConfigs.TREE_HEIGHT + 1) - 1;
		long totalBlocks = totalNodes * TaoConfigs.BLOCKS_IN_BUCKET;
		PrimitiveIterator.OfLong blockIDGenerator = sr.longs(warmupOperations, 0, totalBlocks - 1).iterator();

		for (int i = 0; i < warmupOperations; i++) {
			long blockID = blockIDGenerator.next();
			int readOrWrite = sr.nextInt(2);

			if (readOrWrite == 0) {
				warmUpClient.logicalOperation(blockID, null, false);
			} else {
				byte[] dataToWrite = new byte[TaoConfigs.BLOCK_SIZE];
				Arrays.fill(dataToWrite, (byte) blockID);

				warmUpClient.logicalOperation(blockID, dataToWrite, true);
			}
		}

		TaoLogger.logForce("Beginning load test");

		// inform the proxies we're about to start a load test
		if (clientID == 0) {
			warmUpClient.initProxyLoadTest();
		}

		// Begin actual load test
		ExecutorService clientThreadExecutor = Executors.newFixedThreadPool(concurrentClients,
				Executors.defaultThreadFactory());
		loadTestStartTime = System.currentTimeMillis();
		for (int i = 0; i < concurrentClients; i++) {
			final short j = (short) (i + clientID * concurrentClients);
			Callable<Integer> loadTestClientThread = () -> {
				TaoClient client = new TaoClient(j);

				// Random number generator
				SecureRandom r = new SecureRandom();

				ZipfDistribution zipf = new ZipfDistribution((int) totalBlocks, zipfExp);

				int operationCount = 0;

				while (System.currentTimeMillis() < loadTestStartTime + loadTestLength) {
					int readOrWrite = (r.nextDouble() < rwRatio) ? 0 : 1;
					long targetBlock = zipf.sample();
					boolean success = doLoadTestOperation(client, readOrWrite, targetBlock);
					if (success) {
						operationCount++;
						synchronized (sBucketedThroughputs) {
							int index = (int) ((System.currentTimeMillis() - loadTestStartTime) / SAMPLE_INTERVAL);
							while (sBucketedThroughputs.size() < index + 1) {
								sBucketedThroughputs.add((long) 0);
							}
							sBucketedThroughputs.set(index, sBucketedThroughputs.get(index) + 1);
						}
					}
				}

				synchronized (sThroughputs) {
					sThroughputs.add((double) operationCount);
				}

				return 1;
			};
			clientThreadExecutor.submit(loadTestClientThread);
		}
		clientThreadExecutor.shutdown();
		boolean terminated = clientThreadExecutor.awaitTermination(loadTestLength * 2, TimeUnit.MILLISECONDS);
		long loadTestEndTime = System.currentTimeMillis();
		if (!terminated) {
			TaoLogger.logForce("Clients did not terminate before the timeout elapsed.");
		}

		// inform the proxies that we just finished the load test
		if (clientID == 0) {
			warmUpClient.finishProxyLoadTest();
		}

		// Collections.sort(sResponseTimes);
		// TaoLogger.logForce("Throughputs: " + sThroughputs.toString());
		// TaoLogger.logForce("Response times: " + sResponseTimes.toString());

		TaoLogger.logForce(sProfiler.getClientStatistics());

		TaoLogger.logForce("Response Times over Time:");
		StringBuilder responseTimesBuilder = new StringBuilder();
		for (int i = 0; i < sBucketedResponseTimes.size(); i++) {
			responseTimesBuilder.append(String.format("(%d,%f)", i * SAMPLE_INTERVAL / 1000,
					sBucketedResponseTimes.get(i).stream().mapToDouble(a -> a).average().orElse(0)));
		}
		TaoLogger.logForce(responseTimesBuilder.toString());

		TaoLogger.logForce("Throughputs over Time:");
		StringBuilder throughputsBuilder = new StringBuilder();
		for (int i = 0; i < sBucketedThroughputs.size(); i++) {
			throughputsBuilder.append(String.format("(%d,%f)", i * SAMPLE_INTERVAL / 1000,
					((double) sBucketedThroughputs.get(i)) / (SAMPLE_INTERVAL / 1000)));
		}
		TaoLogger.logForce(throughputsBuilder.toString());

		double throughputTotal = 0;
		for (Double l : sThroughputs) {
			throughputTotal += l;
		}
		double averageThroughput = throughputTotal / ((loadTestEndTime - loadTestStartTime) / 1000);

		TaoLogger.logForce("Ending load test");

		if (sResponseTimes.size() == 0) {
			TaoLogger.logForce("Could not record any response times, try testing for a longer duration.");
			return;
		}

		// Get average response time
		long total = 0;
		for (Long l : sResponseTimes) {
			total += l;
		}
		float average = total / sResponseTimes.size();

		// TaoLogger.logForce("TPS: "+(requestsPerSecond));
		TaoLogger.logForce("Average response time was " + average + " ms");
		TaoLogger.logForce("Median response time was " + sResponseTimes.get(sResponseTimes.size() / 2));
		TaoLogger.logForce("Thoughput: " + averageThroughput);
	}

	public static void main(String[] args) {
		try {
			// Parse any passed in args
			Map<String, String> options = ArgumentParser.parseCommandLineArguments(args);

			// Determine if the user has their own configuration file name, or just use the
			// default
			String configFileName = options.getOrDefault("config_file", TaoConfigs.USER_CONFIG_FILE);
			TaoConfigs.USER_CONFIG_FILE = configFileName;

			// Create client
			short clientID = Short.parseShort(options.getOrDefault("id", "0"));

			// Determine if we are load testing or just making an interactive client
			String runType = options.getOrDefault("runType", "interactive");

			String quorumType = options.getOrDefault("quorum_type", "random");
			if (quorumType.equals("random")) {
				randomQuorum = true;
			} else if (quorumType.equals("nearest")) {
				randomQuorum = false;
			} else {
				TaoLogger.logForce("Unknown quorum_type " + quorumType);
				System.exit(1);
			}

			if (runType.equals("interactive")) {
				Scanner reader = new Scanner(System.in);
				while (true) {
					TaoClient client = new TaoClient(clientID);
					TaoLogger.logForce("W for write, R for read, P for print, Q for quit");
					String option = reader.nextLine();

					if (option.equals("Q")) {
						break;
					} else if (option.equals("W")) {
						TaoLogger.logForce("Enter block ID to write to");
						long blockID = reader.nextLong();

						TaoLogger.logForce("Enter number to fill in block");
						long fill = reader.nextLong();
						byte[] dataToWrite = new byte[TaoConfigs.BLOCK_SIZE];
						Arrays.fill(dataToWrite, (byte) fill);

						TaoLogger.logForce("Going to send write request for " + blockID);
						byte[] writeStatus = client.logicalOperation(blockID, dataToWrite, true);

						if (writeStatus == null) {
							TaoLogger.logForce("Write failed");
							System.exit(1);
						}
					} else if (option.equals("R")) {
						TaoLogger.logForce("Enter block ID to read from");

						long blockID = reader.nextLong();

						TaoLogger.logForce("Going to send read request for " + blockID);
						byte[] result = client.logicalOperation(blockID, null, false);

						if (result != null) {
							TaoLogger.logForce("The result of the read is a block filled with the number " + result[0]);
						} else {
							TaoLogger.logForce("The block is null");
						}
						TaoLogger.logForce("Last number in the block is  " + result[result.length - 1]);
					} else if (option.equals("P")) {
						client.printSubtree();
					}
				}
				reader.close();
			} else {
				TaoConfigs.initConfiguration();
				// Determine number of concurrent clients to run during the load test
				int concurrentClients = Integer
						.parseInt(options.getOrDefault("clients", Integer.toString(CONCURRENT_CLIENTS)));

				// The length of the load test (in ms)
				int loadTestLength = Integer
						.parseInt(options.getOrDefault("load_test_length", Integer.toString(LOAD_TEST_LENGTH)));

				// The number of warmup operations to perform before starting the load test
				int warmupOperations = Integer
						.parseInt(options.getOrDefault("warmup_operations", Integer.toString(WARMUP_OPERATIONS)));

				// print tree size and number of items
				long totalNodes = (long) Math.pow(2, TaoConfigs.TREE_HEIGHT + 1) - 1;
				long numDataItems = totalNodes * TaoConfigs.BLOCKS_IN_BUCKET;
				TaoLogger.logForce("Tree Height: " + TaoConfigs.TREE_HEIGHT);
				TaoLogger.logForce("Number of Data Items: " + numDataItems);

				String rwRatioArg = options.getOrDefault("rwRatio", "0.5");
				double rwRatio = Double.parseDouble(rwRatioArg);

				String zipfExpArg = options.getOrDefault("zipfExp", "1");
				double zipfExp = Double.parseDouble(zipfExpArg);

				loadTest(concurrentClients, loadTestLength, warmupOperations, rwRatio, zipfExp, clientID);

				System.exit(0);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return;
	}

}

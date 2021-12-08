package TaoProxy;

import Configuration.ArgumentParser;
import Configuration.TaoConfigs;
import Configuration.Unit;

import Messages.*;
import TaoClient.OperationID;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.BufferUnderflowException;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.List;
import java.util.ArrayList;

/**
 * @brief Class that represents the proxy which handles requests from clients and replies from servers
 */
public class TaoProxy implements Proxy {
	// Sequencer for proxy
	protected TaoSequencer mSequencer;

	// Processor for proxy
	protected TaoProcessor mProcessor;

	protected TaoInterface mInterface;

	// Thread group for asynchronous sockets
	protected AsynchronousChannelGroup mThreadGroup;

	// A MessageCreator to create different types of messages to be passed from
	// client, proxy, and server
	protected MessageCreator mMessageCreator;

	// A PathCreator
	protected PathCreator mPathCreator;

	// A CryptoUtil
	protected CryptoUtil mCryptoUtil;

	// A Subtree
	protected TaoSubtree mSubtree;

	// A map that maps each leafID to the relative leaf ID it would have within a
	// server partition
	// TODO: Put this in position map?
	protected Map<Long, Long> mRelativeLeafMapper;

	// A position map
	protected PositionMap mPositionMap;

	// A Profiler to store timing information
	public Profiler mProfiler;

	protected int mUnitId;

	// public static final transient ReentrantLock mSubtreeLock = new
	// ReentrantLock();

	protected BlockingQueue<ClientRequest> requestQueue;
	protected ExecutorService requestExecutor;

	// total number of requests that this proxy received from clients
	protected static AtomicLong sNumClientRequests = new AtomicLong();

	/**
	 * @brief Default constructor
	 */
	public TaoProxy() {
	}

	/**
	 * @brief Constructor
	 * @param messageCreator
	 * @param pathCreator
	 * @param subtree
	 */
	public TaoProxy(MessageCreator messageCreator, PathCreator pathCreator, TaoSubtree subtree, int unitId) {
		try {
			// For trace purposes
			TaoLogger.logLevel = TaoLogger.LOG_OFF;

			// For profiling purposes
			mProfiler = new TaoProfiler(unitId);

			// Initialize needed constants
			TaoConfigs.initConfiguration();

			// Create a CryptoUtil
			mCryptoUtil = new TaoCryptoUtil();

			// Assign subtree
			mSubtree = subtree;

			mUnitId = unitId;

			// Create a position map
			Unit u = TaoConfigs.ORAM_UNITS.get(mUnitId);
			List<InetSocketAddress> storageServerAddresses = new ArrayList();
			InetSocketAddress serverAddr = new InetSocketAddress(u.serverHost, u.serverPort);
			storageServerAddresses.add(serverAddr);
			mPositionMap = new TaoPositionMap(storageServerAddresses);

			// Assign the message and path creators
			mMessageCreator = messageCreator;
			mPathCreator = pathCreator;

			// Create a thread pool for asynchronous sockets
			mThreadGroup = AsynchronousChannelGroup.withFixedThreadPool(TaoConfigs.PROXY_THREAD_COUNT,
					Executors.defaultThreadFactory());

			// Map each leaf to a relative leaf for the servers
			mRelativeLeafMapper = new HashMap<>();
			int numServers = 1;
			int numLeaves = 1 << TaoConfigs.TREE_HEIGHT;
			int leavesPerPartition = numLeaves / numServers;
			for (int i = 0; i < numLeaves; i += numLeaves / numServers) {
				long j = i;
				long relativeLeaf = 0;
				while (j < i + leavesPerPartition) {
					mRelativeLeafMapper.put(j, relativeLeaf);
					j++;
					relativeLeaf++;
				}
			}

			// Initialize the sequencer and proxy
			mSequencer = new TaoSequencer(mMessageCreator, mPathCreator);
			mProcessor = new TaoProcessor(this, mSequencer, mThreadGroup, mMessageCreator, mPathCreator, mCryptoUtil,
					mSubtree, mPositionMap, mRelativeLeafMapper, mProfiler, mUnitId);
			mInterface = new TaoInterface(mSequencer, mProcessor, mMessageCreator);
			mSubtree.mInterface = mInterface;
			mSequencer.mProcessor = mProcessor;
			mProcessor.mInterface = mInterface;

			requestQueue = new LinkedBlockingDeque<>();
			requestExecutor = Executors.newFixedThreadPool(TaoConfigs.PROXY_SERVICE_THREADS);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * @brief Function to initialize an empty tree on the server side
	 */
	public void initializeServer() {
		try {
			// Initialize the top of the subtree
			mSubtree.initRoot();

			// Get the total number of paths
			int totalPaths = 1 << TaoConfigs.TREE_HEIGHT;

			TaoLogger.logInfo("Tree height is " + TaoConfigs.TREE_HEIGHT);
			TaoLogger.logInfo("Total paths " + totalPaths);

			// Variables to both hold the data of a path as well as how big the path is

			// Create each connection
			Unit u = TaoConfigs.ORAM_UNITS.get(mUnitId);

			Executor initExecutor = Executors.newFixedThreadPool(TaoConfigs.PROXY_SERVICE_THREADS);
			CompletionService<Integer> initCompletion = new ExecutorCompletionService<Integer>(initExecutor);

			// Loop to write each path to server
			for (int i = 0; i < totalPaths; i++) {
				final int j = i;
				Callable<Integer> writePath = () -> {
					Socket serverSocket = new Socket(u.serverHost, u.serverPort);
					DataOutputStream output = new DataOutputStream(serverSocket.getOutputStream());
					InputStream input = serverSocket.getInputStream();

					// Create empty paths and serialize
					Path defaultPath = mPathCreator.createPath();
					defaultPath.setPathID(mRelativeLeafMapper.get(((long) j)));

					// Encrypt path
					byte[] dataToWrite = mCryptoUtil.encryptPath(defaultPath);

					// Create a proxy write request
					ProxyRequest writebackRequest = mMessageCreator.createProxyRequest();
					writebackRequest.setType(MessageTypes.PROXY_INITIALIZE_REQUEST);
					writebackRequest.setPathSize(dataToWrite.length);
					writebackRequest.setDataToWrite(dataToWrite);

					// Serialize the proxy request
					byte[] proxyRequest = writebackRequest.serialize();

					// Send the type and size of message to server
					byte[] messageTypeBytes = Ints.toByteArray(MessageTypes.PROXY_INITIALIZE_REQUEST);
					byte[] messageLengthBytes = Ints.toByteArray(proxyRequest.length);
					output.write(Bytes.concat(messageTypeBytes, messageLengthBytes));

					// Send actual message to server
					output.write(proxyRequest);

					// Read in the response
					// TODO: Currently not doing anything with response, possibly do something
					byte[] typeAndSize = new byte[8];
					input.read(typeAndSize);
					int type = Ints.fromByteArray(Arrays.copyOfRange(typeAndSize, 0, 4));
					int length = Ints.fromByteArray(Arrays.copyOfRange(typeAndSize, 4, 8));
					byte[] message = new byte[length];
					input.close();
					output.close();
					serverSocket.close();
					return j;
				};
				initCompletion.submit(writePath);
			}

			// wait on the results
			for (int i = 0; i < totalPaths; i++) {
				Future<Integer> result = initCompletion.take();
				Integer pathID = result.get();
				TaoLogger.logForce("Wrote path " + pathID);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void onReceiveRequest(ClientRequest req) {
		// When we receive a request, we first send it to the sequencer
		// mSequencer.onReceiveRequest(req);

		// We send the request to the processor, starting with the read path method
		TaoLogger.logInfo("\n\n\nGot a client request (" + req.getOpID() + ") for " + req.getBlockID());
	}

	@Override
	public void onReceiveResponse(ClientRequest req, ServerResponse resp, boolean isFakeRead) {
		// When a response is received, the processor will answer the request, flush the
		// path, then may perform a
		// write back
		// mSubtreeLock.lock();
		mProcessor.answerRequest(req, resp, isFakeRead);
		TaoLogger.logInfo("Answering a client request for " + req.getBlockID());
		// Even though we flush after o_write we still need to flush here in case the
		// block gets reassigned to a different path and we want to be able to find it
		// at the correct location
		mProcessor.flush(resp.getPathID(), false);
		// mSubtreeLock.unlock();
		mProcessor.writeBack(TaoConfigs.WRITE_BACK_THRESHOLD);
	}

	@Override
	public void run() {
		try {
			Unit u = TaoConfigs.ORAM_UNITS.get(mUnitId);
			// Create an asynchronous channel to listen for connections
			AsynchronousServerSocketChannel channel = AsynchronousServerSocketChannel.open(mThreadGroup);
			channel.bind(new InetSocketAddress(u.proxyPort));
			// Asynchronously wait for incoming connections
			channel.accept(null, new CompletionHandler<AsynchronousSocketChannel, Void>() {
				@Override
				public void completed(AsynchronousSocketChannel clientChannel, Void att) {
					// Start listening for other connections
					channel.accept(null, this);

					// Create new thread that will serve the client
					Runnable serializeProcedure = () -> serveClient(clientChannel);
					new Thread(serializeProcedure).start();
				}

				@Override
				public void failed(Throwable exc, Void att) {
					TaoLogger.logForce("Failed to accept connections");
					exc.printStackTrace();
					try {
						channel.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void processRequests() {
		try {
			while (true) {
				ClientRequest req;
				req = requestQueue.take();
				requestExecutor.submit(() -> mInterface.handleRequest(req));
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private ProxyResponse readProxyResponse(AsynchronousSocketChannel channel) {
		try {
			// Create byte buffer to use to read incoming message type and size
			ByteBuffer messageTypeAndSize = ByteBuffer.allocate(4 + 4);

			// Read the initial header
			Future<?> initRead = channel.read(messageTypeAndSize);
			initRead.get();

			// Flip buffer for reading
			messageTypeAndSize.flip();

			// Parse the message type and size from server
			byte[] messageTypeBytes = new byte[4];
			byte[] messageLengthBytes = new byte[4];
			messageTypeAndSize.get(messageTypeBytes);
			messageTypeAndSize.get(messageLengthBytes);
			int messageType = Ints.fromByteArray(messageTypeBytes);
			int messageLength = Ints.fromByteArray(messageLengthBytes);

			if (messageType != MessageTypes.PROXY_RESPONSE) {
				TaoLogger.logForce("Expected a proxy response but instead got a message of type " + messageType);
				System.exit(1);
			}

			// Clear buffer
			messageTypeAndSize = null;

			// Read rest of message
			ByteBuffer message = ByteBuffer.allocate(messageLength);
			while (message.remaining() > 0) {
				Future<?> entireRead = channel.read(message);
				entireRead.get();
			}

			// Flip buffer for reading
			message.flip();

			// Get bytes from message
			byte[] requestBytes = new byte[messageLength];
			message.get(requestBytes);

			// Clear buffer
			message = null;

			// Initialize ProxyResponse object based on read bytes
			ProxyResponse proxyResponse = mMessageCreator.createProxyResponse();
			proxyResponse.initFromSerialized(requestBytes);
			return proxyResponse;
		} catch (Exception e) {
			try {
				e.printStackTrace();
				channel.close();
				return null;
			} catch (IOException e1) {
				e.printStackTrace();
				return null;
			}
		}
	}

	private void sendRequestToProxy(ClientRequest request, AsynchronousSocketChannel clientChannel) {
		try {
			// Send request to proxy
			byte[] serializedRequest = request.serialize();
			byte[] requestHeader = MessageUtility.createMessageHeaderBytes(request.getType(), serializedRequest.length);
			ByteBuffer requestMessage = ByteBuffer.wrap(Bytes.concat(requestHeader, serializedRequest));

			// Send message to proxy
			TaoLogger.logDebug("Sending request #" + request.getRequestID());
			while (requestMessage.remaining() > 0) {
				Future<Integer> writeResult = clientChannel.write(requestMessage);
				try {
					writeResult.get();
				} catch (Exception e) {
					TaoLogger.logForce("Access daemon was unable to send to the proxy.");
					return;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void accessDaemon() {
		try {
			long blockID = 0;
			long requestNum = 0;
			long requestID;
			// the daemon will have the max client id + 1
			long clientID = TaoConfigs.MAX_CLIENT_ID + 1;
			long totalNodes = (long) Math.pow(2, TaoConfigs.TREE_HEIGHT + 1) - 1;
			long numDataItems = totalNodes * TaoConfigs.BLOCKS_IN_BUCKET;
			InetSocketAddress proxyAddress = new InetSocketAddress(InetAddress.getLocalHost().getHostAddress(),
					TaoConfigs.ORAM_UNITS.get(mUnitId).proxyPort);
			AsynchronousSocketChannel clientChannel = AsynchronousSocketChannel.open(mThreadGroup);
			clientChannel.connect(proxyAddress).get();
			while (true) {
				TaoLogger.logForce("The Daemon is accessing blockID " + blockID);
				// Create read request
				ClientRequest readRequest = mMessageCreator.createClientRequest();
				readRequest.setBlockID(blockID);
				requestID = (Long.highestOneBit(TaoConfigs.MAX_CLIENT_ID + 1) << 1) * (requestNum++) + clientID;
				readRequest.setRequestID(requestID);
				readRequest.setType(MessageTypes.CLIENT_READ_REQUEST);
				readRequest.setClientAddress(proxyAddress);
				readRequest.setChannel(clientChannel);
				readRequest.setData(new byte[TaoConfigs.BLOCK_SIZE]);
				readRequest.setTag(new Tag());
				readRequest.setOpID(new OperationID());
				sendRequestToProxy(readRequest, clientChannel);

				// wait for the proxy to reply to the read
				readProxyResponse(clientChannel);

				// Create write request
				ClientRequest writeRequest = mMessageCreator.createClientRequest();
				writeRequest.setBlockID(blockID);
				requestID = (Long.highestOneBit(TaoConfigs.MAX_CLIENT_ID + 1) << 1) * (requestNum++) + clientID;
				writeRequest.setRequestID(requestID);
				writeRequest.setType(MessageTypes.CLIENT_WRITE_REQUEST);
				writeRequest.setClientAddress(proxyAddress);
				writeRequest.setChannel(clientChannel);
				writeRequest.setData(new byte[TaoConfigs.BLOCK_SIZE]);
				writeRequest.setTag(new Tag());
				writeRequest.setOpID(new OperationID());
				sendRequestToProxy(writeRequest, clientChannel);

				// wait for the proxy to reply to the write
				readProxyResponse(clientChannel);

				blockID = (blockID + 1) % numDataItems;
				Thread.sleep(TaoConfigs.ACCESS_DAEMON_DELAY);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * @brief Method to serve a client connection
	 * @param channel
	 */
	private void serveClient(AsynchronousSocketChannel channel) {
		try {
			TaoLogger.logInfo("Proxy will begin receiving client request");

			// Create a ByteBuffer to read in message type
			ByteBuffer typeByteBuffer = MessageUtility.createTypeReceiveBuffer();

			// Asynchronously read message
			channel.read(typeByteBuffer, null, new CompletionHandler<Integer, Void>() {
				public CompletionHandler<Integer, Void> outerCompletionHandler() {
					return this;
				}

				@Override
				public void completed(Integer result, Void attachment) {
					// Flip the byte buffer for reading
					typeByteBuffer.flip();

					TaoLogger.logInfo("Proxy received a client request");

					// Figure out the type of the message
					int[] typeAndLength;
					try {
						typeAndLength = MessageUtility.parseTypeAndLength(typeByteBuffer);
						typeByteBuffer.clear();
					} catch (BufferUnderflowException e) {
						TaoLogger.logForce("Lost connection to client");
						try {
							// apparently the processor holds a channel open to the server for each client
							// so we need to close those too
							mProcessor.disconnectClient(channel);
							channel.close();
						} catch (IOException e1) {
							e1.printStackTrace();
						}
						return;
					}
					int messageType = typeAndLength[0];
					int messageLength = typeAndLength[1];

					if (messageType == MessageTypes.CLIENT_READ_REQUEST) {
						sNumClientRequests.getAndAdd(1);
					}

					// Serve message based on type
					if (messageType == MessageTypes.CLIENT_WRITE_REQUEST
							|| messageType == MessageTypes.CLIENT_READ_REQUEST) {
						// Get the rest of the message
						ByteBuffer messageByteBuffer = ByteBuffer.allocate(messageLength);

						// Do one last asynchronous read to get the rest of the message
						channel.read(messageByteBuffer, null, new CompletionHandler<Integer, Void>() {
							@Override
							public void completed(Integer result, Void attachment) {
								// Make sure we read all the bytes
								if (messageByteBuffer.remaining() > 0) {
									channel.read(messageByteBuffer, null, this);
									return;
								}

								// start processing the next message
								channel.read(typeByteBuffer, null, outerCompletionHandler());

								// Flip the byte buffer for reading
								messageByteBuffer.flip();

								// Get the rest of the bytes for the message
								byte[] requestBytes = new byte[messageLength];
								messageByteBuffer.get(requestBytes);

								// Create ClientRequest object based on read bytes
								ClientRequest clientReq = mMessageCreator.createClientRequest();
								clientReq.initFromSerialized(requestBytes);
								clientReq.setChannel(channel);

								TaoLogger.logInfo("Proxy will handle client request #" + clientReq.getRequestID());

								// Serve the next client request
								// Runnable serializeProcedure = () -> serveClient(channel);
								// new Thread(serializeProcedure).start();

								// When we receive a request, we first send it to the sequencer
								// mInterface.handleRequest(clientReq);

								try {
									requestQueue.put(clientReq);
								} catch (InterruptedException e) {
									e.printStackTrace();
								}

								// Handle request
								// onReceiveRequest(clientReq);
							}

							@Override
							public void failed(Throwable exc, Void attachment) {
								TaoLogger.logForce("Failed to read a message");
							}
						});
					} else if (messageType == MessageTypes.PRINT_SUBTREE) {
						// Print the subtree, used for debugging
						mSubtree.printSubtree();
						channel.read(typeByteBuffer, null, this);
					} else if (messageType == MessageTypes.WRITE_STATS) {
						mProfiler.writeStatistics();
						channel.read(typeByteBuffer, null, this);
					} else if (messageType == MessageTypes.INIT_LOAD_TEST) {
						sNumClientRequests.set(0);
						mProcessor.initLoadTest();
						channel.read(typeByteBuffer, null, this);
					} else if (messageType == MessageTypes.FINISH_LOAD_TEST) {
						TaoLogger.logForce("Total Client Requests: " + sNumClientRequests.get());
						mProcessor.finishLoadTest();
						channel.read(typeByteBuffer, null, this);
					}
				}

				@Override
				public void failed(Throwable exc, Void attachment) {
					exc.printStackTrace();
					return;
				}
			});
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
	}

	public static void main(String[] args) {
		try {
			// Parse any passed in args
			Map<String, String> options = ArgumentParser.parseCommandLineArguments(args);

			// Determine if the user has their own configuration file name, or just use the
			// default
			String configFileName = options.getOrDefault("config_file", TaoConfigs.USER_CONFIG_FILE);
			TaoConfigs.USER_CONFIG_FILE = configFileName;

			// Get the ORAM unit id
			int unitId = Integer.parseInt(options.get("unit"));

			// Create proxy
			TaoProxy proxy = new TaoProxy(new TaoMessageCreator(), new TaoBlockCreator(), new TaoSubtree(), unitId);

			// Initialize and run server
			proxy.initializeServer();
			TaoLogger.logForce("Finished init, running proxy");
			// launch the consumer thread
			new Thread(() -> proxy.processRequests()).start();
			// launch the producer thread
			proxy.run();
			// launch the path access daemon
			new Thread(() -> proxy.accessDaemon()).start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

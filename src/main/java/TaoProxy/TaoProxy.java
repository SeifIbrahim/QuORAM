package TaoProxy;

import Configuration.ArgumentParser;
import Configuration.TaoConfigs;
import Configuration.Unit;

import Messages.*;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;

import java.io.DataOutputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.List;
import java.util.ArrayList;

/**
 * @brief Class that represents the proxy which handles requests from clients and replies from servers
 */
public class TaoProxy implements Proxy {
    // Sequencer for proxy
    protected Sequencer mSequencer;

    // Processor for proxy
    protected Processor mProcessor;

    // Thread group for asynchronous sockets
    protected AsynchronousChannelGroup mThreadGroup;

    // A MessageCreator to create different types of messages to be passed from client, proxy, and server
    protected MessageCreator mMessageCreator;

    // A PathCreator
    protected PathCreator mPathCreator;

    // A CryptoUtil
    protected CryptoUtil mCryptoUtil;

    // A Subtree
    protected Subtree mSubtree;

    // A map that maps each leafID to the relative leaf ID it would have within a server partition
    // TODO: Put this in position map?
    protected Map<Long, Long> mRelativeLeafMapper;

    // A position map
    protected PositionMap mPositionMap;

    // A Profiler to store timing information
    public Profiler mProfiler;

    protected int mUnitId;

    //public static final transient ReentrantLock mSubtreeLock = new ReentrantLock();

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
    public TaoProxy(MessageCreator messageCreator, PathCreator pathCreator, Subtree subtree, int unitId) {
        try {
            // For trace purposes
            TaoLogger.logLevel = TaoLogger.LOG_INFO;

            // For profiling purposes
            mProfiler = new TaoProfiler();

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
            mThreadGroup = AsynchronousChannelGroup.withFixedThreadPool(TaoConfigs.PROXY_THREAD_COUNT, Executors.defaultThreadFactory());

            // Map each leaf to a relative leaf for the servers
            mRelativeLeafMapper = new HashMap<>();
            int numServers = 1;
            int numLeaves = 1 << TaoConfigs.TREE_HEIGHT;
            int leavesPerPartition = numLeaves / numServers;
            for (int i = 0; i < numLeaves; i += numLeaves/numServers) {
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
            mProcessor = new TaoProcessor(this, mSequencer, mThreadGroup, mMessageCreator, mPathCreator, mCryptoUtil, mSubtree, mPositionMap, mRelativeLeafMapper, mProfiler, mUnitId);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @brief Constructor
     * @param messageCreator
     * @param pathCreator
     * @param subtree
     * @param processor
     */
    public TaoProxy(MessageCreator messageCreator, PathCreator pathCreator, Subtree subtree, Processor processor, int unitId) {
        try {
            mUnitId = unitId;

            // For trace purposes
            TaoLogger.logLevel = TaoLogger.LOG_INFO;

            // For profiling purposes
            mProfiler = new TaoProfiler();

            // Initialize needed constants
           // TaoConfigs.initConfiguration(minServerSize);

            // Create a CryptoUtil
            mCryptoUtil = new TaoCryptoUtil();

            // Assign subtree
            mSubtree = subtree;

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
            mThreadGroup = AsynchronousChannelGroup.withFixedThreadPool(TaoConfigs.PROXY_THREAD_COUNT, Executors.defaultThreadFactory());

            // Initialize the sequencer and proxy
            mSequencer = new TaoSequencer(mMessageCreator, mPathCreator);
            mProcessor = processor;

            // Map each leaf to a relative leaf for the servers
            mRelativeLeafMapper = new HashMap<>();
            int numServers = 1;
            int numLeaves = 1 << TaoConfigs.TREE_HEIGHT;
            int leavesPerPartition = numLeaves / numServers;
            for (int i = 0; i < numLeaves; i += numLeaves/numServers) {
                long j = i;
                long relativeLeaf = 0;
                while (j < i + leavesPerPartition) {
                    mRelativeLeafMapper.put(j, relativeLeaf);
                    j++;
                    relativeLeaf++;
                }
            }
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
            byte[] dataToWrite;

            // Create each connection
            Unit u = TaoConfigs.ORAM_UNITS.get(mUnitId);
            Socket serverSocket = new Socket(u.serverHost, u.serverPort);

            // Loop to write each path to server
            for (int i = 0; i < totalPaths; i++) {
                TaoLogger.logForce("Creating path " + i);

                DataOutputStream output = new DataOutputStream(serverSocket.getOutputStream());
                InputStream input = serverSocket.getInputStream();

                // Create empty paths and serialize
                Path defaultPath = mPathCreator.createPath();
                defaultPath.setPathID(mRelativeLeafMapper.get(((long) i)));

                // Encrypt path
                dataToWrite = mCryptoUtil.encryptPath(defaultPath);

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
                input.read(message);
            }

            serverSocket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onReceiveRequest(ClientRequest req) {
        // When we receive a request, we first send it to the sequencer
        // mSequencer.onReceiveRequest(req);

        // We send the request to the processor, starting with the read path method
        TaoLogger.logInfo("Got a client request for "+req.getBlockID());
        mProcessor.readPath(req);
    }

    @Override
    public void onReceiveResponse(ClientRequest req, ServerResponse resp, boolean isFakeRead) {
        // When a response is received, the processor will answer the request, flush the path, then may perform a
        // write back
        //mSubtreeLock.lock();
        mProcessor.answerRequest(req, resp, isFakeRead);
        TaoLogger.logInfo("Answering a client request for "+req.getBlockID());
        mProcessor.flush(resp.getPathID());
        //mSubtreeLock.unlock();
        mProcessor.writeBack(TaoConfigs.WRITE_BACK_THRESHOLD);
    }

    @Override
    public void run() {
        try {
            Unit u = TaoConfigs.ORAM_UNITS.get(mUnitId);
            // Create an asynchronous channel to listen for connections
            AsynchronousServerSocketChannel channel =
                    AsynchronousServerSocketChannel.open(mThreadGroup).bind(new InetSocketAddress(u.proxyPort));

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
                    // TODO: implement?
                }
            });
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
                @Override
                public void completed(Integer result, Void attachment) {
                    // Flip the byte buffer for reading
                    typeByteBuffer.flip();

                    TaoLogger.logDebug("Proxy received a client request");

                    // Figure out the type of the message
                    int[] typeAndLength = MessageUtility.parseTypeAndLength(typeByteBuffer);
                    int messageType = typeAndLength[0];
                    int messageLength = typeAndLength[1];

                    // Serve message based on type
                    if (messageType == MessageTypes.CLIENT_WRITE_REQUEST || messageType == MessageTypes.CLIENT_READ_REQUEST) {
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

                                // Create ClientRequest object based on read bytes
                                ClientRequest clientReq = mMessageCreator.createClientRequest();
                                clientReq.initFromSerialized(requestBytes);

                                TaoLogger.logDebug("Proxy will handle client request #" + clientReq.getRequestID());

                                // When we receive a request, we first send it to the sequencer
                                mSequencer.onReceiveRequest(clientReq);

                                // Serve the next client request
                                Runnable serializeProcedure = () -> serveClient(channel);
                                new Thread(serializeProcedure).start();

                                // Handle request
                                onReceiveRequest(clientReq);
                            }

                            @Override
                            public void failed(Throwable exc, Void attachment) {
                                // TODO: implement?
                            }
                        });

                    } else if (messageType == MessageTypes.PRINT_SUBTREE) {
                        // Print the subtree, used for debugging
                        mSubtree.printSubtree();
                    } else if (messageType == MessageTypes.WRITE_STATS) {
                        mProfiler.writeStatistics();
                    }
                }
                @Override
                public void failed(Throwable exc, Void attachment) {
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

            // Determine if the user has their own configuration file name, or just use the default
            String configFileName = options.getOrDefault("config_file", TaoConfigs.USER_CONFIG_FILE);
            TaoConfigs.USER_CONFIG_FILE = configFileName;

            // Get the ORAM unit id
            int unitId = Integer.parseInt(options.get("unit"));

            // Create proxy
            TaoProxy proxy = new TaoProxy(new TaoMessageCreator(), new TaoBlockCreator(), new TaoSubtree(), unitId);

            // Initialize and run server
            proxy.initializeServer();
            TaoLogger.logForce("Finished init, running proxy");
            proxy.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

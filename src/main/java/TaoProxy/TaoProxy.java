package TaoProxy;

import Configuration.ArgumentParser;
import Configuration.TaoConfigs;
import Configuration.Unit;

import Messages.*;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;

import java.util.concurrent.*;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.BufferUnderflowException;
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

import org.mapdb.*;

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

    protected DB mKVStore;

    protected ConcurrentMap mKVMap;

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
            mProfiler = new TaoProfiler(unitId);

            // Initialize needed constants
            TaoConfigs.initConfiguration();

            mUnitId = unitId;

            // Assign the message and path creators
            mMessageCreator = messageCreator;

            // Create a thread pool for asynchronous sockets
            mThreadGroup = AsynchronousChannelGroup.withFixedThreadPool(TaoConfigs.PROXY_THREAD_COUNT, Executors.defaultThreadFactory());

            mKVStore = DBMaker.fileDB("oram"+mUnitId+".db").closeOnJvmShutdown().make();
            mKVMap = mKVStore.hashMap("map", Serializer.LONG, Serializer.BYTE_ARRAY).createOrOpen();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onReceiveRequest(ClientRequest req) {
        // Create response
        ProxyResponse response = mMessageCreator.createProxyResponse();
        response.setClientRequestID(req.getRequestID());

        if (req.getType() == MessageTypes.CLIENT_READ_REQUEST) {
            System.out.println("Got read request");
            byte[] blockBytes = (byte[])mKVMap.getOrDefault(req.getBlockID(), null);
            Block b = new TaoBlock();
            if (blockBytes != null) {
                b.initFromSerialized(blockBytes);
            }
            response.setReturnTag(b.getTag());
            response.setReturnData(b.getData());
            response.setWriteStatus(false);
        } else if (req.getType() == MessageTypes.CLIENT_WRITE_REQUEST) {
            byte[] blockBytes = (byte[])mKVMap.getOrDefault(req.getBlockID(), null);
            Block b = new TaoBlock();
            if (blockBytes != null) {
                b.initFromSerialized(blockBytes);
            }
            if (req.getTag().compareTo(b.getTag()) > 0) {
                b.setData(req.getData());
                b.setTag(req.getTag());
                b.setBlockID(req.getBlockID());
                mKVMap.put(req.getBlockID(), b.serialize());
            }
            response.setWriteStatus(true);
            response.setReturnTag(new Tag());
            System.out.println("Wrote block "+req.getBlockID());
        } 
        

        // Get channel
        AsynchronousSocketChannel clientChannel = req.getChannel();

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
                    int[] typeAndLength;
                    try {
                        typeAndLength = MessageUtility.parseTypeAndLength(typeByteBuffer);
                    } catch (BufferUnderflowException e) {
                        System.out.println("Lost connection to client");
                        return;
                    }
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
                                clientReq.setChannel(channel);

                                TaoLogger.logDebug("Proxy will handle client request #" + clientReq.getRequestID());

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

            TaoLogger.logForce("Finished init, running proxy");
            proxy.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

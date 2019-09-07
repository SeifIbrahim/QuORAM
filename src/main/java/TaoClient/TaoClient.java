package TaoClient;

import Configuration.ArgumentParser;
import Configuration.TaoConfigs;
import Messages.*;
import TaoProxy.*;
import com.google.common.primitives.Bytes;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.BufferUnderflowException;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.LongStream;

/**
 * @brief Class to represent a client of TaoStore
 */
public class TaoClient implements Client {
    // The address of the proxy
    protected List<InetSocketAddress> mProxyAddresses;

    // The address of this client
    protected InetSocketAddress mClientAddress;

    // A MessageCreator to create different types of messages to be passed from client, proxy, and server
    protected MessageCreator mMessageCreator;

    // Counter to keep track of current request number
    // Incremented after each request
    protected AtomicLong mRequestID;

    // Thread group for asynchronous sockets
    protected AsynchronousChannelGroup mThreadGroup;

    // Map of request IDs to ProxyResponses. Used to differentiate which request a response is answering
    protected Map<Long, ProxyResponse> mResponseWaitMap;

    // Channel to proxy
    protected Map<Integer, AsynchronousSocketChannel> mChannels;

    // ExecutorService for async reads/writes
    protected ExecutorService mExecutor;

    // Next operation ID to use
    protected OperationID mNextOpID;

    /* Below static variables are used for load testing*/

    public static Short sNextClientID = 0;

    // Used for measuring latency when running load test
    public static List<Long> sResponseTimes = new ArrayList<>();

    // Used for locking the async load test until all the operations are replied to
    public static Object sAsycLoadLock = new Object();

    // List of bytes used for writing blocks as well as comparing the results of returned block data
    public static ArrayList<byte[]> sListOfBytes = new ArrayList<>();

    // Map used during async load tests to map a blockID to the bytes that it should be compared to upon proxy reply
    public static Map<Long, Integer> sReturnMap = new HashMap<>();

    // Number of operations in load test
    public static int LOAD_SIZE = 1000;

    // Number of unique data items in load test
    public static int NUM_DATA_ITEMS = 1000;

    // Whether or not a load test is for an async load or not
    public static boolean ASYNC_LOAD = false;

    public static ArrayList<Double> sThroughputs = new ArrayList<>();

    public static long loadTestStartTime;

    public short mClientID;

    public Map<Integer, Long> mBackoffTimeMap = new HashMap<>();
    public Map<Integer, Integer> mBackoffCountMap = new HashMap<>();
    public ReadWriteLock backoffLock = new ReentrantReadWriteLock();

    public TaoClient(short id) {
        try {
            System.out.println("making client");
            // Initialize needed constants
            TaoConfigs.initConfiguration();

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

            // Initialize list of proxy addresses
            mProxyAddresses = new ArrayList();
            for (int i = 0; i < TaoConfigs.ORAM_UNITS.size(); i++) {
                mProxyAddresses.add(new InetSocketAddress(TaoConfigs.ORAM_UNITS.get(i).proxyHost, TaoConfigs.ORAM_UNITS.get(i).proxyPort));
                mBackoffTimeMap.put(i, new Long(0));
                mBackoffCountMap.put(i, 0);
            }

            // Create message creator
            mMessageCreator = new TaoMessageCreator();

            // Initialize response wait map
            mResponseWaitMap = new ConcurrentHashMap<>();

            // Thread group used for asynchronous I/O
            mThreadGroup = AsynchronousChannelGroup.withFixedThreadPool(TaoConfigs.PROXY_THREAD_COUNT, Executors.defaultThreadFactory());

            boolean connected = false;
            mChannels = new HashMap();
            while (!connected) {
                try {
                    // Create and connect channels to proxies
                    for (int i = 0; i < TaoConfigs.ORAM_UNITS.size(); i++) {
                        AsynchronousSocketChannel channel = AsynchronousSocketChannel.open(mThreadGroup);
                        Future connection = channel.connect(mProxyAddresses.get(i));
                        connection.get();
                        mChannels.put(i, channel);
                    }
                    connected = true;
                } catch (Exception e) {
                    try {
                    } catch (Exception e2) {
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e3) {
                    }
                }
            }

            // Create executor
            mExecutor = Executors.newFixedThreadPool(TaoConfigs.PROXY_THREAD_COUNT, Executors.defaultThreadFactory());

            // Request ID counter
            mRequestID = new AtomicLong();

            Runnable serializeProcedure = () -> processAllProxyReplies();
            new Thread(serializeProcedure).start();
            System.out.println("made client");
        } catch (Exception e) {
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
        Iterator it = mChannels.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, AsynchronousSocketChannel> entry = (Map.Entry)it.next();
            System.out.println("Setting up thread for process "+entry.getKey());
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
                    System.out.println("Unit "+unitID+" is down");
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException interruptError) {
                    }
                    AsynchronousSocketChannel newChannel;

                    System.out.println("Trying to reconnect to unit "+unitID);
                    boolean connected = false;
                    while (!connected) {
                        try {
                            channel.close();
                            newChannel = AsynchronousSocketChannel.open(mThreadGroup);
                            Future connection = newChannel.connect(mProxyAddresses.get(unitID));
                            connection.get();
                            System.out.println("Successfully reconnected to unit "+unitID);
                            mChannels.put(unitID, newChannel);
                            processProxyReplies(unitID);
                            return;
                        } catch (Exception connError) {
                        }
                    }
                    //TODO: restore connection and recurse
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

                            // Notify thread waiting for this response id
                            synchronized (clientAnswer) {
                                clientAnswer.notifyAll();
                                mResponseWaitMap.remove(clientAnswer.getClientRequestID());

                                if (ASYNC_LOAD) {

                                    // If this is an async load, we need to notify the test that we are done
                                    if (clientAnswer.getClientRequestID() == (NUM_DATA_ITEMS + LOAD_SIZE - 1)) {
                                        synchronized (sAsycLoadLock) {
                                            sAsycLoadLock.notifyAll();
                                        }
                                    }

                                    // Check for correctness
                                    if (sReturnMap.get(clientAnswer.getClientRequestID()) != null) {
                                        if (!Arrays.equals(sListOfBytes.get(sReturnMap.get(clientAnswer.getClientRequestID())), clientAnswer.getReturnData())) {
                                            TaoLogger.logError("Read failed for block " + sReturnMap.get(clientAnswer.getClientRequestID()));
                                            System.exit(1);
                                        }
                                    }
                                } else {
                                }

                                processProxyReplies(unitID);
                            }
                        }

                        @Override
                        public void failed(Throwable exc, Void attachment) {
                        }
                    });
                }
            }
            @Override
            public void failed(Throwable exc, Void attachment) {
            }
        });

    }

    public int selectUnit(Set<Integer> quorum) {
        // Find the ORAM units which can be contacted at this time
        ArrayList<Integer> available = new ArrayList<>();

        while (available.isEmpty()) {
            backoffLock.readLock().lock();

            Long time = System.currentTimeMillis();
            Long soonestBackoffTime = new Long(-1);
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

        return available.get((new Random()).nextInt(available.size()));
    }

    public Set<Integer> buildQuorum() {
        Set<Integer> quorum = new LinkedHashSet<>();

        while (quorum.size() < (int)((TaoConfigs.ORAM_UNITS.size()+1)/2)) {
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
        long backoff = (long)Math.pow(2, mBackoffCountMap.get(unitID)) + 500 + (new Random().nextInt(500));
        System.out.println("Unit "+unitID+" will not be contacted for "+backoff+"ms");
        mBackoffTimeMap.put(unitID, time + backoff);
        backoffLock.writeLock().unlock();
    }

    public byte[] logicalOperation(long blockID, byte[] data, boolean isWrite) {
        System.out.println("\n\n");

        System.out.println("starting logical op");
        // Assign the operation a unique ID
        OperationID opID;
        synchronized (mNextOpID) {
            opID = mNextOpID;
            mNextOpID = mNextOpID.getNext();
        }

        // Select the initial quorum for this operation
        System.out.println("Quorum contains:");
        Set<Integer> quorum = buildQuorum();
        for (int i : quorum) {
            System.out.println(i);
        }

        System.out.println("Starting logical operation "+opID);

        // Broadcast read(blockID) to all ORAM units
        Map<Integer, Long> timeStart = new HashMap<>();
        Map<Integer, Future<ProxyResponse>> readResponsesWaiting = new HashMap();
        for (int i : quorum) {
            readResponsesWaiting.put(i, readAsync(blockID, i, opID));
            timeStart.put(i, System.currentTimeMillis());
        }

        byte[] writebackVal = null;
        Tag tag = null;

        // Wait for a read quorum (here a simple majority) of responses
        int responseCount = 0;
        while(responseCount < (int)((TaoConfigs.ORAM_UNITS.size()+1)/2)) {
            while (quorum.size() < (int)((TaoConfigs.ORAM_UNITS.size()+1)/2)) {
                System.out.println("Adding unit to quorum");
                int addedUnit = selectUnit(quorum);
                quorum.add(addedUnit);
                readResponsesWaiting.put(addedUnit, readAsync(blockID, addedUnit, opID));
                timeStart.put(addedUnit, System.currentTimeMillis());
            }

            Iterator it = readResponsesWaiting.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Integer, Future<ProxyResponse>> entry = (Map.Entry)it.next();
                if (entry.getValue().isDone()) {
                    try {
                        System.out.println("From proxy "+entry.getKey() +": got value "+entry.getValue().get().getReturnData()[0]+" with tag "+entry.getValue().get().getReturnTag());
                        byte[] val = entry.getValue().get().getReturnData();
                        Tag responseTag = entry.getValue().get().getReturnTag();
                        
                        // Set writeback value to the value with the greatest tag
                        if (tag == null || responseTag.compareTo(tag) >= 0) {
                            writebackVal = val;
                            tag = responseTag;
                        }
                        
                        it.remove();
                        responseCount++;

                        markResponsive(entry.getKey());
                    } catch (Exception e) {
                        System.out.println(e);
                        e.printStackTrace();
                    }
                } else if (System.currentTimeMillis() > timeStart.get(entry.getKey()) + 2000) {
                    entry.getValue().cancel(true);
                    System.out.println("Timed out waiting for proxy "+entry.getKey());
                    it.remove();

                    // Remove unit from quorum and mark unresponsive
                    quorum.remove(entry.getKey());
                    markUnresponsive(entry.getKey());
                }
            }
        }

        // Cancel all pending reads, so that threads are not wasted
        Iterator it = readResponsesWaiting.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, Future<ProxyResponse>> entry = (Map.Entry)it.next();
            entry.getValue().cancel(true);
            it.remove();
        }

        // If write, set writeback value to client's value
        // and increment tag
        if (isWrite) {
            writebackVal = data;
            tag.seqNum = tag.seqNum + 1;
            tag.clientID = mClientID;
        }

        // Broadcast write(blockID, writeback value, writeback tag)
        // to all ORAM units
        Map<Integer, Future<ProxyResponse>> writeResponsesWaiting = new HashMap();
        for (int i : quorum) {
            writeResponsesWaiting.put(i, writeAsync(blockID, writebackVal, tag, i, opID));
            timeStart.put(i, System.currentTimeMillis());
        }

        // Wait for a write quorum of acks (here a simple majority)
        responseCount = 0;
        while(responseCount < (int)((TaoConfigs.ORAM_UNITS.size()+1)/2)) {
            it = writeResponsesWaiting.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Integer, Future<ProxyResponse>> entry = (Map.Entry)it.next();
                if (entry.getValue().isDone()) {
                    try {
                        System.out.println("Got ack from proxy "+entry.getKey());
                        it.remove();
                        responseCount++;
                    } catch (Exception e) {
                        System.out.println(e);
                    }
                } else if (System.currentTimeMillis() > timeStart.get(entry.getKey()) + 5000) {
                    entry.getValue().cancel(true);
                    System.out.println("Timed out waiting for proxy "+entry.getKey());

                    return null;
                }
            }
        }

        // Cancel all pending writes, so that threads are not wasted
        it = writeResponsesWaiting.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, Future<ProxyResponse>> entry = (Map.Entry)it.next();
            entry.getValue().cancel(true);
            it.remove();
        }

        System.out.println("\n\n");

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
    protected ClientRequest makeRequest(int type, long blockID, byte[] data, Tag tag, OperationID opID, List<Object> extras) {
        // Keep track of requestID and increment it
        // Because request IDs must be globally unique, we bit-shift the
        // request ID to the left and add the client ID, thereby
        // ensuring that no two clients use the same request ID
        int shift = (int)Math.ceil(Math.log(TaoConfigs.MAX_CLIENT_ID)/Math.log(2));
        long requestID = (long)Math.pow(2, shift) * mRequestID.getAndAdd(1) + mClientID;

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
            if (ASYNC_LOAD) {
                sReturnMap.put(requestID, (int) blockID - 1);
            }
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
     * @brief Private helper method to send a read or write request to a TaoStore proxy
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
                        System.out.println("Unable to contact unit "+unitID);
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

    @Override
    public void printSubtree() {
        try {
            // Get proxy name and port
            Socket clientSocket = new Socket(mProxyAddresses.get(0).getHostName(), mProxyAddresses.get(0).getPort());

            // Create output stream
            DataOutputStream output = new DataOutputStream(clientSocket.getOutputStream());

            // Create client request
            ClientRequest request = mMessageCreator.createClientRequest();
            request.setType(MessageTypes.PRINT_SUBTREE);

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
    public void writeStatistics(int unitID) {
        try {
            // Get proxy name and port
            Socket clientSocket = new Socket(mProxyAddresses.get(unitID).getHostName(), mProxyAddresses.get(unitID).getPort());

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

    public static int doLoadTestOperation(Client client, int readOrWrite, long targetBlock) {
        byte[] z;
        if (readOrWrite == 0) {
            TaoLogger.logInfo("Doing read request #" + ((TaoClient) client).mRequestID.get());

            // Send read and keep track of response time
            long start = System.currentTimeMillis();

            z = client.logicalOperation(targetBlock, null, false);
            sResponseTimes.add(System.currentTimeMillis() - start);
        } else {
            TaoLogger.logInfo("Doing write request #" + ((TaoClient) client).mRequestID.get());

            // Send write and keep track of response time
            byte[] dataToWrite = new byte[TaoConfigs.BLOCK_SIZE];
            Arrays.fill(dataToWrite, (byte) targetBlock);

            long start = System.currentTimeMillis();
            boolean writeStatus = (client.logicalOperation(targetBlock, dataToWrite, true) != null);
            sResponseTimes.add(System.currentTimeMillis() - start);

            if (!writeStatus) {
                TaoLogger.logError("Write failed for block " + targetBlock);
                System.exit(1);
            }
        }

        return 1;
    }

    public static void loadTest() throws InterruptedException {
        int concurrentClients = 3;
        // Length of load test in ms
        int loadTestLength = 1000 * 60 * 2;
        // Number of operations to do for warmup
        int warmUpOperations = 100;
        // Unit to use in no-replication load test
        int unitToUse = 0;

        Callable<Integer> loadTestClientThread = () -> {
            TaoClient client = new TaoClient((short)-1); 

            // Random number generator
            SecureRandom r = new SecureRandom();

            long totalNodes = (long)Math.pow(2,TaoConfigs.TREE_HEIGHT + 1) - 1;
            long totalBlocks = totalNodes * TaoConfigs.BLOCKS_IN_BUCKET;

            int operationCount = 0;

            while (System.currentTimeMillis() < loadTestStartTime + loadTestLength) {
                int readOrWrite = r.nextInt(2);
                long targetBlock = r.nextLong()%totalBlocks;
                long opStartTime = System.currentTimeMillis();
                doLoadTestOperation(client, readOrWrite, targetBlock);
                long latency = System.currentTimeMillis() - opStartTime;
                if (latency < 200) {
                        Thread.sleep(200 - latency);
                }
                operationCount++;
            }

            synchronized (sThroughputs) {
                sThroughputs.add((double)operationCount);
            }

            return 1;
        };

        // Warm up the system
        TaoClient warmUpClient = new TaoClient((short)-1);
        SecureRandom r = new SecureRandom();
        long totalNodes = (long)Math.pow(2,TaoConfigs.TREE_HEIGHT + 1) - 1;
        long totalBlocks = totalNodes * TaoConfigs.BLOCKS_IN_BUCKET;
        PrimitiveIterator.OfLong blockIDGenerator = r.longs(warmUpOperations, 0, totalBlocks - 1).iterator();

        for (int i = 0; i < warmUpOperations; i++) {
            long blockID = blockIDGenerator.next();
            int readOrWrite = r.nextInt(2);

            if (readOrWrite == 0) {
                warmUpClient.logicalOperation(blockID, null, false);
            } else {
                byte[] dataToWrite = new byte[TaoConfigs.BLOCK_SIZE];
                Arrays.fill(dataToWrite, (byte) blockID);

                warmUpClient.logicalOperation(blockID, dataToWrite, true);
            }
        }

        // Begin actual load test
        ExecutorService clientThreadExecutor = Executors.newFixedThreadPool(concurrentClients, Executors.defaultThreadFactory());
        loadTestStartTime = System.currentTimeMillis();
        for (int i = 0; i < concurrentClients; i++) {
            clientThreadExecutor.submit(loadTestClientThread);
        }
        clientThreadExecutor.shutdown();
        clientThreadExecutor.awaitTermination(loadTestLength*2, TimeUnit.MILLISECONDS);

        double throughputTotal = 0;
        for (Double l : sThroughputs) {
            throughputTotal += l;
        }
        double averageThroughput = throughputTotal / (loadTestLength/1000);


        TaoLogger.logForce("Ending load test");

        // Get average response time
        long total = 0;
        for (Long l : sResponseTimes) {
            total += l;
        }
        float average = total / sResponseTimes.size();

        //TaoLogger.logForce("TPS: "+(requestsPerSecond));
        TaoLogger.logForce("Average response time was " + average + " ms");
        TaoLogger.logForce("Thoughput: " + averageThroughput);
    }



    public static void main(String[] args) {
        try {
            // Parse any passed in args
            Map<String, String> options = ArgumentParser.parseCommandLineArguments(args);

            // Determine if the user has their own configuration file name, or just use the default
            String configFileName = options.getOrDefault("config_file", TaoConfigs.USER_CONFIG_FILE);
            TaoConfigs.USER_CONFIG_FILE = configFileName;

            // Create client
            
            String clientID = options.get("id");

            // Determine if we are load testing or just making an interactive client
            String runType = options.getOrDefault("runType", "interactive");

            if (runType.equals("interactive")) {
                Scanner reader = new Scanner(System.in);
                while (true) {
                    TaoClient client = new TaoClient(Short.parseShort(clientID));
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
            } else {
                // Determine if we are doing a load test with synchronous operations, or asynchronous
                String load_test_type = options.getOrDefault("load_test_type", "synchronous");

                // Determine the amount of operations in the load test
                String load_size = options.getOrDefault("load_size", Integer.toString(LOAD_SIZE));
                LOAD_SIZE = Integer.parseInt(load_size);

                // Determine the amount of unique data items that can be operated on
                String data_set_size = options.getOrDefault("data_set_size", Integer.toString(NUM_DATA_ITEMS));
                NUM_DATA_ITEMS = Integer.parseInt(data_set_size);

                if (load_test_type.equals("synchronous")) {
                    loadTest();
                }

                System.exit(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return;
    }


}

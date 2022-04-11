package TaoClient;

import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousSocketChannel;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.math3.distribution.ZipfDistribution;

import Configuration.ArgumentParser;
import Configuration.TaoConfigs;
import TaoProxy.TaoLogger;

/**
 * @brief: This client talks directly to the TaoServer
 * but it is still referred to as Proxy in the code to avoid code duplication
 */
public class InsecureTaoClient extends TaoClient {

	public InsecureTaoClient(short id) {
		super(id);
		TaoLogger.logLevel = TaoLogger.LOG_OFF;
	}

	protected void initializeConnections() {
		// Initialize list of SERVER addresses
		mProxyAddresses = new ArrayList<InetSocketAddress>();
		for (int i = 0; i < TaoConfigs.ORAM_UNITS.size(); i++) {
			mProxyAddresses.add(new InetSocketAddress(TaoConfigs.ORAM_UNITS.get(i).serverHost,
					TaoConfigs.ORAM_UNITS.get(i).serverPort));
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

	// change static methods to say InsecureTaoClient instead of TaoClient
	public static int doLoadTestOperation(Client client, int readOrWrite, long targetBlock) {
		if (readOrWrite == 0) {
			TaoLogger.logInfo("Doing read request #" + ((InsecureTaoClient) client).mRequestID.get());

			// Send read and keep track of response time
			long start = System.currentTimeMillis();

			client.logicalOperation(targetBlock, null, false);
			synchronized (sResponseTimes) {
				sResponseTimes.add(System.currentTimeMillis() - start);
			}
		} else {
			TaoLogger.logInfo("Doing write request #" + ((InsecureTaoClient) client).mRequestID.get());

			// Send write and keep track of response time
			byte[] dataToWrite = new byte[TaoConfigs.BLOCK_SIZE];
			Arrays.fill(dataToWrite, (byte) targetBlock);

			long start = System.currentTimeMillis();
			boolean writeStatus = (client.logicalOperation(targetBlock, dataToWrite, true) != null);
			synchronized (sResponseTimes) {
				sResponseTimes.add(System.currentTimeMillis() - start);
			}

			if (!writeStatus) {
				TaoLogger.logForce("Write failed for block " + targetBlock);
				System.exit(1);
			}
		}

		return 1;
	}

	public static void loadTest(int concurrentClients, int loadTestLength, int warmupOperations, double rwRatio,
			double zipfExp) throws InterruptedException {
		Callable<Integer> loadTestClientThread = () -> {
			InsecureTaoClient client = new InsecureTaoClient((short) -1);

			// Random number generator
			SecureRandom r = new SecureRandom();

			long totalNodes = (long) Math.pow(2, TaoConfigs.TREE_HEIGHT + 1) - 1;
			long totalBlocks = totalNodes * TaoConfigs.BLOCKS_IN_BUCKET;

			ZipfDistribution zipf = new ZipfDistribution((int) totalBlocks, zipfExp);

			int operationCount = 0;

			while (System.currentTimeMillis() < loadTestStartTime + loadTestLength) {
				int readOrWrite = (r.nextDouble() < rwRatio) ? 0 : 1;
				long targetBlock = zipf.sample();
				doLoadTestOperation(client, readOrWrite, targetBlock);
				operationCount++;
			}

			synchronized (sThroughputs) {
				sThroughputs.add((double) operationCount);
			}

			return 1;
		};

		// Warm up the system
		InsecureTaoClient warmUpClient = new InsecureTaoClient((short) -1);
		SecureRandom r = new SecureRandom();
		long totalNodes = (long) Math.pow(2, TaoConfigs.TREE_HEIGHT + 1) - 1;
		long totalBlocks = totalNodes * TaoConfigs.BLOCKS_IN_BUCKET;
		PrimitiveIterator.OfLong blockIDGenerator = r.longs(warmupOperations, 0, totalBlocks - 1).iterator();

		for (int i = 0; i < warmupOperations; i++) {
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

		TaoLogger.logForce("Beginning load test");

		// Begin actual load test
		ExecutorService clientThreadExecutor = Executors.newFixedThreadPool(concurrentClients,
				Executors.defaultThreadFactory());
		loadTestStartTime = System.currentTimeMillis();
		for (int i = 0; i < concurrentClients; i++) {
			clientThreadExecutor.submit(loadTestClientThread);
		}
		clientThreadExecutor.shutdown();
		boolean terminated = clientThreadExecutor.awaitTermination(loadTestLength * 2, TimeUnit.MILLISECONDS);
		long loadTestEndTime = System.currentTimeMillis();
		if (!terminated) {
			TaoLogger.logForce("Clients did not terminate before the timeout elapsed.");
		}

		// Collections.sort(sResponseTimes);
		// TaoLogger.logForce("Throughputs: " + sThroughputs.toString());
		// TaoLogger.logForce("Response times: " + sResponseTimes.toString());

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

			String clientID = options.get("id");

			// Determine if we are load testing or just making an interactive client
			String runType = options.getOrDefault("runType", "interactive");

			if (runType.equals("interactive")) {
				Scanner reader = new Scanner(System.in);
				while (true) {
					InsecureTaoClient client = new InsecureTaoClient(Short.parseShort(clientID));
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

				loadTest(concurrentClients, loadTestLength, warmupOperations, rwRatio, zipfExp);

				System.exit(0);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return;
	}
}

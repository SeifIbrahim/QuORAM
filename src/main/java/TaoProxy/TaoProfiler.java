package TaoProxy;

import Messages.ClientRequest;
import Messages.MessageTypes;
import TaoClient.OperationID;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

public class TaoProfiler implements Profiler {

	protected String mOutputDirectory;

	// Proxy Profiling
	protected DescriptiveStatistics mReadPathStatistics;
	protected DescriptiveStatistics mWriteBackStatistics;

	protected DescriptiveStatistics mReadPathSendToRecvStatistics;
	protected DescriptiveStatistics mWriteBackSendToRecvStatistics;

	protected DescriptiveStatistics mReadPathProcessingStatistics;
	protected DescriptiveStatistics mWriteBackProcessingStatistics;

	protected DescriptiveStatistics mReadPathNetStatistics;
	protected DescriptiveStatistics mWriteBackNetStatistics;

	protected DescriptiveStatistics mAddPathStatistics;

	protected Map<Long, Long> mWriteBackStartTimes;

	protected Map<InetSocketAddress, Map<Integer, Long>> mReadPathPreSendTimes;
	protected Map<InetSocketAddress, Map<Long, Long>> mWriteBackPreSendTimes;

	protected Map<InetSocketAddress, Map<Integer, Long>> mReadPathSendToRecvTimes;
	protected Map<InetSocketAddress, Map<Long, Long>> mWriteBackSendToRecvTimes;

	protected Map<Integer, Long> mProxyMovingReadStartTimes;
	protected Map<Integer, Long> mProxyMovingWriteStartTimes;

	protected Map<Integer, Long> mProxyReadStartTimes;
	protected DescriptiveStatistics mProxyReadStatistics;

	protected Map<Integer, Long> mProxyWriteStartTimes;
	protected DescriptiveStatistics mProxyWriteStatistics;

	protected DescriptiveStatistics mInterfaceReadStatistics;
	protected DescriptiveStatistics mInterfaceWriteStatistics;

	protected DescriptiveStatistics mAnswerRequestStatistics;

	protected DescriptiveStatistics mSequencerStatistics;

	// Client Profiling
	protected Map<OperationID, Long> mReadQuorumPreSendTimes;
	protected DescriptiveStatistics mReadQuorumSendToRecvStatistics;

	protected Map<OperationID, Long> mWriteQuorumPreSendTimes;
	protected DescriptiveStatistics mWriteQuorumSendToRecvStatistics;

	protected Map<Integer, Map<Long, Long>> mClientReadStartTimes;
	protected Map<Integer, Map<Long, Long>> mClientWriteStartTimes;

	protected Map<Integer, Map<Long, Long>> mClientReadSendToRecvTimes;
	protected Map<Integer, Map<Long, Long>> mClientWriteSendToRecvTimes;

	protected Map<Integer, DescriptiveStatistics> mClientReadSendToRecvStatistics;
	protected Map<Integer, DescriptiveStatistics> mClientWriteSendToRecvStatistics;

	protected Map<Integer, DescriptiveStatistics> mProxyReadProcessingStatistics;
	protected Map<Integer, DescriptiveStatistics> mProxyWriteProcessingStatistics;

	protected Map<Integer, DescriptiveStatistics> mProxyReadNetStatistics;
	protected Map<Integer, DescriptiveStatistics> mProxyWriteNetStatistics;

	protected int unitID;

	public TaoProfiler(int uID) {
		unitID = uID;

		mOutputDirectory = "profile";

		// Proxy Profiling
		mReadPathStatistics = new DescriptiveStatistics();
		mWriteBackStatistics = new DescriptiveStatistics();

		mReadPathSendToRecvStatistics = new DescriptiveStatistics();
		mWriteBackSendToRecvStatistics = new DescriptiveStatistics();

		mReadPathProcessingStatistics = new DescriptiveStatistics();
		mWriteBackProcessingStatistics = new DescriptiveStatistics();

		mReadPathNetStatistics = new DescriptiveStatistics();
		mWriteBackNetStatistics = new DescriptiveStatistics();

		mAddPathStatistics = new DescriptiveStatistics();

		mWriteBackStartTimes = new ConcurrentHashMap<>();

		mReadPathPreSendTimes = new ConcurrentHashMap<>();
		mWriteBackPreSendTimes = new ConcurrentHashMap<>();

		mReadPathSendToRecvTimes = new ConcurrentHashMap<>();
		mWriteBackSendToRecvTimes = new ConcurrentHashMap<>();

		mProxyMovingReadStartTimes = new ConcurrentHashMap<>();
		mProxyMovingWriteStartTimes = new ConcurrentHashMap<>();

		mProxyReadStartTimes = new ConcurrentHashMap<>();
		mProxyReadStatistics = new DescriptiveStatistics();

		mProxyWriteStartTimes = new ConcurrentHashMap<>();
		mProxyWriteStatistics = new DescriptiveStatistics();

		mInterfaceReadStatistics = new DescriptiveStatistics();
		mInterfaceWriteStatistics = new DescriptiveStatistics();

		mAnswerRequestStatistics = new DescriptiveStatistics();

		mSequencerStatistics = new DescriptiveStatistics();

		// Client Profiling
		mReadQuorumPreSendTimes = new ConcurrentHashMap<>();
		mReadQuorumSendToRecvStatistics = new DescriptiveStatistics();

		mWriteQuorumPreSendTimes = new ConcurrentHashMap<>();
		mWriteQuorumSendToRecvStatistics = new DescriptiveStatistics();

		mClientReadStartTimes = new ConcurrentHashMap<>();
		mClientWriteStartTimes = new ConcurrentHashMap<>();

		mClientReadSendToRecvTimes = new ConcurrentHashMap<>();
		mClientWriteSendToRecvTimes = new ConcurrentHashMap<>();

		mClientReadSendToRecvStatistics = new ConcurrentHashMap<>();
		mClientWriteSendToRecvStatistics = new ConcurrentHashMap<>();

		mProxyReadProcessingStatistics = new ConcurrentHashMap<>();
		mProxyWriteProcessingStatistics = new ConcurrentHashMap<>();

		mProxyReadNetStatistics = new ConcurrentHashMap<>();
		mProxyWriteNetStatistics = new ConcurrentHashMap<>();
	}

	private String oneLineStats(DescriptiveStatistics descriptiveStatistics) {
		return String.format("%,.2f/%,.2f/%,.2f/%,.2f", descriptiveStatistics.getMean(), descriptiveStatistics.getMin(),
				descriptiveStatistics.getMax(), descriptiveStatistics.getStandardDeviation());
	}

	public String getClientStatistics() {
		StringBuilder sb = new StringBuilder();
		sb.append(String.format("%n%-20s%s%n", "Read Quorum: ", oneLineStats(mReadQuorumSendToRecvStatistics)));
		for (Entry<Integer, DescriptiveStatistics> e : mClientReadSendToRecvStatistics.entrySet()) {
			sb.append(String.format("\t%-20s%s%n", "Proxy " + e.getKey() + ": ", oneLineStats(e.getValue())));
			sb.append(String.format("\t\t%-20s%s%n", "Processing Time: ",
					oneLineStats(mProxyReadProcessingStatistics.get(e.getKey()))));
			sb.append(String.format("\t\t%-20s%s%n", "Network Time: ",
					oneLineStats(mProxyReadNetStatistics.get(e.getKey()))));
		}

		sb.append(String.format("%n%-20s%s%n", "Write Quorum: ", oneLineStats(mWriteQuorumSendToRecvStatistics)));
		for (Entry<Integer, DescriptiveStatistics> e : mClientWriteSendToRecvStatistics.entrySet()) {
			sb.append(String.format("\t%-20s%s%n", "Proxy " + e.getKey() + ": ", oneLineStats(e.getValue())));
			sb.append(String.format("\t\t%-20s%s%n", "Processing Time: ",
					oneLineStats(mProxyWriteProcessingStatistics.get(e.getKey()))));
			sb.append(String.format("\t\t%-20s%s%n", "Network Time: ",
					oneLineStats(mProxyWriteNetStatistics.get(e.getKey()))));
		}
		return sb.toString();
	}

	public String getProxyStatistics() {
		StringBuilder sb = new StringBuilder();
		sb.append(String.format("%n%-20s%s%n", "Read Request: ", oneLineStats(mProxyReadStatistics)));
		sb.append(String.format("\t%-20s%s%n", "Reached Interface: ", oneLineStats(mInterfaceReadStatistics)));
		sb.append(String.format("\t%-20s%s%n", "ReadPath Complete: ", oneLineStats(mReadPathStatistics)));
		sb.append(String.format("\t\t%-20s%s%n", "ReadPath Request: ", oneLineStats(mReadPathSendToRecvStatistics)));
		sb.append(
				String.format("\t\t\t%-20s%s%n", "ReadPath Processing: ", oneLineStats(mReadPathProcessingStatistics)));
		sb.append(String.format("\t\t\t%-20s%s%n", "ReadPath Network: ", oneLineStats(mReadPathNetStatistics)));
		sb.append(String.format("\t%-20s%s%n", "AnswerRequest Complete: ", oneLineStats(mAnswerRequestStatistics)));
		sb.append(String.format("\t\t%-20s%s%n", "AddPath Time: ", oneLineStats(mAddPathStatistics)));
		sb.append(String.format("\t%-20s%s%n", "Sequencer Fetch: ", oneLineStats(mSequencerStatistics)));

		sb.append(String.format("%n%n%-20s%s%n", "Write Request: ", oneLineStats(mProxyWriteStatistics)));
		sb.append(String.format("\t%-20s%s%n", "Reached Interface: ", oneLineStats(mInterfaceWriteStatistics)));
		sb.append(String.format("%n%n%-20s%s%n", "WriteBack Function: ", oneLineStats(mWriteBackStatistics)));
		sb.append(String.format("\t%-20s%s%n", "WriteBack Request: ", oneLineStats(mWriteBackSendToRecvStatistics)));
		sb.append(
				String.format("\t\t%-20s%s%n", "WriteBack Processing: ", oneLineStats(mWriteBackProcessingStatistics)));
		sb.append(String.format("\t\t%-20s%s%n", "WriteBack Network: ", oneLineStats(mWriteBackNetStatistics)));
		return sb.toString();
	}

	public void writeStatistics() {
		String report = null;
		String filename = null;

		filename = "readPathStats" + unitID + ".txt";
		synchronized (mReadPathStatistics) {
			report = mReadPathStatistics.toString();
		}
		// Write the report to a file
		try {
			PrintWriter writer = new PrintWriter(filename);
			writer.println(report);
			writer.close();
		} catch (IOException e) {
			TaoLogger.logForce(e.getMessage());
		}

		filename = "writeBackStats" + unitID + ".txt";
		synchronized (mWriteBackStatistics) {
			report = mWriteBackStatistics.toString();
		}
		// Write the report to a file
		try {
			PrintWriter writer = new PrintWriter(filename);
			writer.println(report);
			writer.close();
		} catch (IOException e) {
			TaoLogger.logForce(e.getMessage());
		}

		filename = "readPathSendToRecvStats" + unitID + ".txt";
		synchronized (mReadPathSendToRecvStatistics) {
			report = mReadPathSendToRecvStatistics.toString();
		}
		// Write the report to a file
		try {
			PrintWriter writer = new PrintWriter(filename);
			writer.println(report);
			writer.close();
		} catch (IOException e) {
			TaoLogger.logForce(e.getMessage());
		}

		filename = "writeBackSendToRecvStats" + unitID + ".txt";
		synchronized (mWriteBackSendToRecvStatistics) {
			report = mWriteBackSendToRecvStatistics.toString();
		}
		// Write the report to a file
		try {
			PrintWriter writer = new PrintWriter(filename);
			writer.println(report);
			writer.close();
		} catch (IOException e) {
			TaoLogger.logForce(e.getMessage());
		}

		filename = "readPathServerProcessingStats" + unitID + ".txt";
		synchronized (mReadPathProcessingStatistics) {
			report = mReadPathProcessingStatistics.toString();
		}
		// Write the report to a file
		try {
			PrintWriter writer = new PrintWriter(filename);
			writer.println(report);
			writer.close();
		} catch (IOException e) {
			TaoLogger.logForce(e.getMessage());
		}

		filename = "writeBackServerProcessingStats" + unitID + ".txt";
		synchronized (mWriteBackProcessingStatistics) {
			report = mWriteBackProcessingStatistics.toString();
		}
		// Write the report to a file
		try {
			PrintWriter writer = new PrintWriter(filename);
			writer.println(report);
			writer.close();
		} catch (IOException e) {
			TaoLogger.logForce(e.getMessage());
		}

		filename = "readPathNetStats" + unitID + ".txt";
		synchronized (mReadPathNetStatistics) {
			report = mReadPathNetStatistics.toString();
		}
		// Write the report to a file
		try {
			PrintWriter writer = new PrintWriter(filename);
			writer.println(report);
			writer.close();
		} catch (IOException e) {
			TaoLogger.logForce(e.getMessage());
		}

		filename = "writeBackNetStats" + unitID + ".txt";
		synchronized (mWriteBackNetStatistics) {
			report = mWriteBackNetStatistics.toString();
		}
		// Write the report to a file
		try {
			PrintWriter writer = new PrintWriter(filename);
			writer.println(report);
			writer.close();
		} catch (IOException e) {
			TaoLogger.logForce(e.getMessage());
		}

		filename = "addPathStats" + unitID + ".txt";
		synchronized (mAddPathStatistics) {
			report = mAddPathStatistics.toString();
		}
		// Write the report to a file
		try {
			PrintWriter writer = new PrintWriter(filename);
			writer.println(report);
			writer.close();
		} catch (IOException e) {
			TaoLogger.logForce(e.getMessage());
		}
	}

	@Override
	public void proxyOperationStart(ClientRequest req) {
		long time = System.currentTimeMillis();
		if (req.getType() == MessageTypes.CLIENT_READ_REQUEST) {
			mProxyMovingReadStartTimes.put(req.hashCode(), time);
			mProxyReadStartTimes.put(req.hashCode(), time);
		} else {
			mProxyMovingWriteStartTimes.put(req.hashCode(), time);
			mProxyWriteStartTimes.put(req.hashCode(), time);
		}
	}

	@Override
	public void reachedInterface(ClientRequest clientReq) {
		long t2 = System.currentTimeMillis();
		if (clientReq.getType() == MessageTypes.CLIENT_READ_REQUEST) {
			long t1 = mProxyMovingReadStartTimes.get(clientReq.hashCode());
			synchronized (mInterfaceReadStatistics) {
				mInterfaceReadStatistics.addValue(t2 - t1);
			}
			// move the start time forward for later profiling
			mProxyMovingReadStartTimes.put(clientReq.hashCode(), t2);
		} else {
			long t1 = mProxyMovingWriteStartTimes.get(clientReq.hashCode());
			synchronized (mInterfaceWriteStatistics) {
				mInterfaceWriteStatistics.addValue(t2 - t1);
			}
			mProxyMovingWriteStartTimes.put(clientReq.hashCode(), t2);
		}
	}

	public void readPathComplete(ClientRequest req) {
		long t2 = System.currentTimeMillis();
		long t1 = mProxyMovingReadStartTimes.get(req.hashCode());
		synchronized (mReadPathStatistics) {
			mReadPathStatistics.addValue(t2 - t1);
		}
		mProxyMovingReadStartTimes.put(req.hashCode(), t2);
	}

	@Override
	public void answerRequestComplete(ClientRequest req) {
		long t2 = System.currentTimeMillis();
		long t1 = mProxyMovingReadStartTimes.get(req.hashCode());
		synchronized (mAnswerRequestStatistics) {
			mAnswerRequestStatistics.addValue(t2 - t1);
		}
		mProxyMovingReadStartTimes.put(req.hashCode(), t2);
	}

	@Override
	public long proxyOperationComplete(ClientRequest req) {
		long time = System.currentTimeMillis();
		long totalProcessingTime;
		if (req.getType() == MessageTypes.CLIENT_READ_REQUEST) {
			totalProcessingTime = time - mProxyReadStartTimes.remove(req.hashCode());
			synchronized (mProxyReadStatistics) {
				mProxyReadStatistics.addValue(totalProcessingTime);
			}
			long sequencerTime = time - mProxyMovingReadStartTimes.remove(req.hashCode());
			synchronized (mSequencerStatistics) {
				mSequencerStatistics.addValue(sequencerTime);
			}
		} else {
			totalProcessingTime = time - mProxyWriteStartTimes.remove(req.hashCode());
			synchronized (mProxyWriteStatistics) {
				mProxyWriteStatistics.addValue(totalProcessingTime);
			}
		}
		return totalProcessingTime;
	}

	public void writeBackStart(long writeBackTime) {
		mWriteBackStartTimes.put(writeBackTime, System.currentTimeMillis());
	}

	public void writeBackComplete(long writeBackTime) {
		long writeBackStartTime = mWriteBackStartTimes.remove(writeBackTime);
		synchronized (mWriteBackStatistics) {
			mWriteBackStatistics.addValue(System.currentTimeMillis() - writeBackStartTime);
		}
	}

	public void readPathServerProcessingTime(InetSocketAddress address, ClientRequest req, long processingTime) {
		Map<Integer, Long> readPathSendToRecvTimesForServer = mReadPathSendToRecvTimes.get(address);
		long t2 = readPathSendToRecvTimesForServer.remove(req.hashCode());
		long netTimeApprox = t2 - processingTime;

		synchronized (mReadPathProcessingStatistics) {
			mReadPathProcessingStatistics.addValue(processingTime);
		}

		synchronized (mReadPathNetStatistics) {
			mReadPathNetStatistics.addValue(netTimeApprox);
		}
	}

	public void writeBackServerProcessingTime(InetSocketAddress address, long writeBackTime, long processingTime) {
		Map<Long, Long> writeBackSendToRecvTimesForServer = mWriteBackSendToRecvTimes.get(address);
		long t2 = writeBackSendToRecvTimesForServer.remove(writeBackTime);
		long netTimeApprox = t2 - processingTime;

		synchronized (mWriteBackProcessingStatistics) {
			mWriteBackProcessingStatistics.addValue(processingTime);
		}

		synchronized (mWriteBackNetStatistics) {
			mWriteBackNetStatistics.addValue(netTimeApprox);
		}
	}

	public void readPathPreSend(InetSocketAddress address, ClientRequest req) {
		mReadPathPreSendTimes.putIfAbsent(address, new ConcurrentHashMap<>());

		Map<Integer, Long> serverReadPathPreSendTimes = mReadPathPreSendTimes.get(address);
		serverReadPathPreSendTimes.put(req.hashCode(), System.currentTimeMillis());
	}

	public void readPathPostRecv(InetSocketAddress address, ClientRequest req) {
		long t2 = System.currentTimeMillis();
		mReadPathSendToRecvTimes.putIfAbsent(address, new ConcurrentHashMap<>());

		Map<Integer, Long> readPathSendToRecvTimesForServer = mReadPathSendToRecvTimes.get(address);
		long t1 = mReadPathPreSendTimes.get(address).remove(req.hashCode());

		synchronized (mReadPathSendToRecvStatistics) {
			mReadPathSendToRecvStatistics.addValue(t2 - t1);
		}
		// TaoLogger.logForce("readPathSendToRecv time (" + address + ", " +
		// req.getRequestID() + "): " + (t2-t1));
		readPathSendToRecvTimesForServer.put(req.hashCode(), t2 - t1);
	}

	public void writeBackPreSend(InetSocketAddress address, long writeBackTime) {
		mWriteBackPreSendTimes.putIfAbsent(address, new ConcurrentHashMap<>());

		Map<Long, Long> serverWriteBackPreSendTimes = mWriteBackPreSendTimes.get(address);
		serverWriteBackPreSendTimes.put(writeBackTime, System.currentTimeMillis());
	}

	public void writeBackPostRecv(InetSocketAddress address, long writeBackTime) {
		long t2 = System.currentTimeMillis();
		mWriteBackSendToRecvTimes.putIfAbsent(address, new ConcurrentHashMap<>());

		Map<Long, Long> writeBackSendToRecvTimesForServer = mWriteBackSendToRecvTimes.get(address);
		long t1 = mWriteBackPreSendTimes.get(address).remove(writeBackTime);

		synchronized (mWriteBackSendToRecvStatistics) {
			mWriteBackSendToRecvStatistics.addValue(t2 - t1);
		}
		writeBackSendToRecvTimesForServer.put(writeBackTime, t2 - t1);
	}

	public void addPathTime(long processingTime) {
		synchronized (mAddPathStatistics) {
			mAddPathStatistics.addValue(processingTime);
		}
	}

	@Override
	public void readQuorumPreSend(OperationID opID) {
		mReadQuorumPreSendTimes.put(opID, System.currentTimeMillis());
	}

	@Override
	public void readQuorumPostRecv(OperationID opID) {
		long t2 = System.currentTimeMillis();
		long t1 = mReadQuorumPreSendTimes.remove(opID);
		synchronized (mReadQuorumSendToRecvStatistics) {
			mReadQuorumSendToRecvStatistics.addValue(t2 - t1);
		}
	}

	@Override
	public void writeQuorumPreSend(OperationID opID) {
		mWriteQuorumPreSendTimes.put(opID, System.currentTimeMillis());
	}

	@Override
	public void writeQuorumPostRecv(OperationID opID) {
		long t2 = System.currentTimeMillis();
		long t1 = mWriteQuorumPreSendTimes.remove(opID);
		synchronized (mWriteQuorumSendToRecvStatistics) {
			mWriteQuorumSendToRecvStatistics.addValue(t2 - t1);
		}
	}

	@Override
	public void clientRequestPreSend(long clientRequestID, boolean write, int unitID) {
		if (write) {
			mClientWriteStartTimes.putIfAbsent(unitID, new ConcurrentHashMap<>());
			Map<Long, Long> proxyStartTimes = mClientWriteStartTimes.get(unitID);
			proxyStartTimes.put(clientRequestID, System.currentTimeMillis());
		} else {
			mClientReadStartTimes.putIfAbsent(unitID, new ConcurrentHashMap<>());
			Map<Long, Long> proxyStartTimes = mClientReadStartTimes.get(unitID);
			proxyStartTimes.put(clientRequestID, System.currentTimeMillis());
		}
	}

	@Override
	public void clientRequestPostRecv(long clientRequestID, boolean write, int unitID) {
		long t2 = System.currentTimeMillis();
		if (write) {
			mClientWriteSendToRecvTimes.putIfAbsent(unitID, new ConcurrentHashMap<>());
			Map<Long, Long> sendToRecvTimesForProxy = mClientWriteSendToRecvTimes.get(unitID);

			long t1 = mClientWriteStartTimes.get(unitID).remove(clientRequestID);
			sendToRecvTimesForProxy.put(clientRequestID, t2 - t1);

			mClientWriteSendToRecvStatistics.putIfAbsent(unitID, new DescriptiveStatistics());
			synchronized (mClientWriteSendToRecvStatistics.get(unitID)) {
				mClientWriteSendToRecvStatistics.get(unitID).addValue(t2 - t1);
			}
		} else {
			mClientReadSendToRecvTimes.putIfAbsent(unitID, new ConcurrentHashMap<>());
			Map<Long, Long> sendToRecvTimesForProxy = mClientReadSendToRecvTimes.get(unitID);

			long t1 = mClientReadStartTimes.get(unitID).remove(clientRequestID);
			sendToRecvTimesForProxy.put(clientRequestID, t2 - t1);

			mClientReadSendToRecvStatistics.putIfAbsent(unitID, new DescriptiveStatistics());
			synchronized (mClientReadSendToRecvStatistics.get(unitID)) {
				mClientReadSendToRecvStatistics.get(unitID).addValue(t2 - t1);
			}
		}
	}

	@Override
	public void proxyProcessingTime(long clientRequestID, boolean write, int unitID, long processingTime) {
		if (write) {
			long netTimeApprox = mClientWriteSendToRecvTimes.get(unitID).remove(clientRequestID) - processingTime;

			mProxyWriteProcessingStatistics.putIfAbsent(unitID, new DescriptiveStatistics());
			synchronized (mProxyWriteProcessingStatistics.get(unitID)) {
				mProxyWriteProcessingStatistics.get(unitID).addValue(processingTime);
			}

			mProxyWriteNetStatistics.putIfAbsent(unitID, new DescriptiveStatistics());
			synchronized (mProxyWriteNetStatistics.get(unitID)) {
				mProxyWriteNetStatistics.get(unitID).addValue(netTimeApprox);
			}
		} else {
			long netTimeApprox = mClientReadSendToRecvTimes.get(unitID).remove(clientRequestID) - processingTime;

			mProxyReadProcessingStatistics.putIfAbsent(unitID, new DescriptiveStatistics());
			synchronized (mProxyReadProcessingStatistics.get(unitID)) {
				mProxyReadProcessingStatistics.get(unitID).addValue(processingTime);
			}

			mProxyReadNetStatistics.putIfAbsent(unitID, new DescriptiveStatistics());
			synchronized (mProxyReadNetStatistics.get(unitID)) {
				mProxyReadNetStatistics.get(unitID).addValue(netTimeApprox);
			}
		}
	}

}
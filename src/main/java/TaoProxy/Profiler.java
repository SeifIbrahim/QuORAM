package TaoProxy;

import Messages.ClientRequest;
import TaoClient.OperationID;

import java.net.InetSocketAddress;

public interface Profiler {
    void writeStatistics();

    void readPathStart(ClientRequest req);

    void readPathComplete(ClientRequest req);

    void writeBackStart(long writeBackTime);

    void writeBackComplete(long writeBackTime);

    void readPathServerProcessingTime(InetSocketAddress address, ClientRequest req, long processingTime);

    void writeBackServerProcessingTime(InetSocketAddress address, long writeBackTime, long processingTime);

    void readPathPreSend(InetSocketAddress address, ClientRequest req);

    void readPathPostRecv(InetSocketAddress address, ClientRequest req);

    void writeBackPreSend(InetSocketAddress address, long writeBackTime);

    void writeBackPostRecv(InetSocketAddress address, long writeBackTime);

    void addPathTime(long processingTime);

	void readQuorumPreSend(OperationID opID);

	void readQuorumPostRecv(OperationID opID);

	void writeQuorumPreSend(OperationID opID);

	void writeQuorumPostRecv(OperationID opID);

	void proxyOperationStart(ClientRequest req);

	// returns total time that read/write request spent in the proxy
	long proxyOperationComplete(ClientRequest req);

	void clientRequestPreSend(long requestID, boolean write, int unitID);

	void clientRequestPostRecv(long clientRequestID, boolean write, int unitID);

	void proxyProcessingTime(long clientRequestID, boolean write, int unitID, long processingTime);

	String getClientStatistics();

	String getProxyStatistics();
}

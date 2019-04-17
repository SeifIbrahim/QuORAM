package TaoClient;

import TaoProxy.Tag;
import Messages.ProxyResponse;

import java.util.concurrent.Future;

/**
 * @brief Interface for a TaoStore Client
 */
public interface Client {
    /**
     * @brief Synchronously read data from proxy
     * @param blockID
     * @return the data in block with block id == blockID
     */
    byte[] read(long blockID, int unitID);

    /**
     * @brief Synchronously write data to proxy
     * @param blockID
     * @param data
     * @return if write was successful
     */
    boolean write(long blockID, byte[] data, int unitID);

    /**
     * @brief Asynchronously read data from proxy
     * @param blockID
     * @return a Future that will eventually have the data from block with block id == blockID
     */
    Future<ProxyResponse> readAsync(long blockID, int unitID, OperationID opID);

    /**
     * @brief Asynchronously write data to proxy
     * @param blockID
     * @param data
     * @return a Future that will eventually return a boolean revealing if the write was successful
     */
    Future<ProxyResponse> writeAsync(long blockID, byte[] data, Tag tag, int unitID, OperationID opID);

    byte[] logicalOperation(long blockID, byte[] data, boolean isWrite);

    /**
     * @brief Ask proxy to print its subtree. Used for debugging
     */
    void printSubtree();

    /**
     * @brief Ask proxy to print its statistical information. Used for profiling
     */
    void writeStatistics(int unitID);
}

package TaoClient;


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
    Future<byte[]> readAsync(long blockID, int unitID);

    /**
     * @brief Asynchronously write data to proxy
     * @param blockID
     * @param data
     * @return a Future that will eventually return a boolean revealing if the write was successful
     */
    Future<Boolean> writeAsync(long blockID, byte[] data, int unitID);

    /**
     * @brief Ask proxy to print it's subtree. Used for debugging
     */
    void printSubtree();

    /**
     * @brief Ask proxy to print it's statistical information. Used for profiling
     */
    void writeStatistics();
}

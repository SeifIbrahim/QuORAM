package TaoProxy;

import Messages.ClientRequest;
import Messages.ServerResponse;

/**
 * @brief Interface for Processor
 */
public interface Processor {
    /**
     * @brief Method to read path from server when given a request from sequencer
     * @param req
     */
    void readPath(ClientRequest req);

    /**
     * @brief Method to answer the request made by the sequencer
     * @param req
     * @param resp
     * @param isFakeRead
     */
    void answerRequest(ClientRequest req, ServerResponse resp, boolean isFakeRead);

    /**
     * @brief Flush stash to path
     * @param pathID
     */
    void flush(long pathID, boolean update);

    /**
     * @brief Write paths from subtree back to server
     * @param timeStamp
     */
    void writeBack(long timeStamp);

    void writeDataToBlock(long blockID, byte[] data, Tag tag);
}

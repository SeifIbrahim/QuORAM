package TaoProxy;

import TaoClient.OperationID;
import Messages.ClientRequest;

import java.util.HashMap;
import java.util.Map;

public class TaoInterface {
    // Maps from operation ID to block ID
    public Map<OperationID, Long> mIncompleteCache = new HashMap<>();

    // Maps from block ID to how many ongoing operations involve that block.
    // This must be changed each time an operation is added to
    // or removed from the incompleteCache. When the number drops to 0,
    // the block ID must be removed from the map.
    public Map<Long, Integer> mBlocksInCache = new HashMap<>();

    protected Sequencer mSequencer;

    public TaoInterface(Sequencer s) {
        mSequencer = s;
    }

    public void handleRequest(ClientRequest clientReq) {
        mSequencer.onReceiveRequest(clientReq);
    }

}

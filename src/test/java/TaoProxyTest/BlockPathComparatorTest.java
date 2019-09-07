package TaoProxyTest;

import Configuration.TaoConfigs;
import TaoProxy.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.PriorityQueue;

import static org.junit.Assert.assertEquals;

/**
 * @brief
 */
public class BlockPathComparatorTest {
    @Test
    public void testHeap() {
        long targetPath = 0;
        TaoPositionMap map = new TaoPositionMap();
        Integer[] paths = new Integer[] {0, 8, 2, 1, 4};
        int old_tree_height = TaoConfigs.TREE_HEIGHT;
        TaoConfigs.TREE_HEIGHT = 4;
        ArrayList<Block> blocks = new ArrayList<>(paths.length);

        for (Integer i : paths) {
            blocks.add(new TaoBlock((long) i));
            map.setBlockPosition((long) i, (long) i);
        }

        PriorityQueue<Block> blockHeap = new PriorityQueue<>(TaoConfigs.BLOCKS_IN_BUCKET, new BlockPathComparator(targetPath, map));

        blockHeap.addAll(blocks);

        Long[] answer = new Long[] {0L, 1L, 2L, 4L, 8L};
        int i = 0;
        while (!blockHeap.isEmpty()) {
            assertEquals((long) answer[i], blockHeap.poll().getBlockID());
            i++;
        }

        TaoConfigs.TREE_HEIGHT = old_tree_height;
    }
}
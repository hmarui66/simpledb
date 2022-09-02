package index.hash;

import index.Index;
import record.Layout;
import tx.Transaction;

public class HashIndex implements Index {
    public static int NUM_BUCKETS = 100;
    public HashIndex(Transaction tx, String idxName, Layout idxLayout) {
    }

    public static int searchCost(int numBlocks, int rpb) {
        return numBlocks / HashIndex.NUM_BUCKETS;
    }
}

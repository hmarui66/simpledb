package index.hash;

import index.Index;
import query.Constant;
import record.Layout;
import record.RID;
import tx.Transaction;

public class HashIndex implements Index {
    public static int NUM_BUCKETS = 100;
    public HashIndex(Transaction tx, String idxName, Layout idxLayout) {
    }

    public static int searchCost(int numBlocks, int rpb) {
        return numBlocks / HashIndex.NUM_BUCKETS;
    }

    @Override
    public void beforeFirst(Constant searchKey) {

    }

    @Override
    public boolean next() {
        return false;
    }

    @Override
    public RID getDataRid() {
        return null;
    }

    @Override
    public void insert(Constant dataVal, RID dataRid) {

    }

    @Override
    public void delete(Constant dataVal, RID dataRid) {

    }

    @Override
    public void close() {

    }
}

package simpledb.index.hash;

import simpledb.index.Index;
import simpledb.query.Constant;
import simpledb.record.Layout;
import simpledb.record.RID;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

public class HashIndex implements Index {
    public static int NUM_BUCKETS = 100;
    private Transaction tx;
    private String idxName;
    private Layout layout;
    private Constant searchKey = null;
    private TableScan ts = null;

    public HashIndex(Transaction tx, String idxName, Layout layout) {
        this.tx = tx;
        this.idxName = idxName;
        this.layout = layout;
    }


    @Override
    public void beforeFirst(Constant searchKey) {
        close();
        this.searchKey = searchKey;
        int bucket = searchKey.hashCode() % NUM_BUCKETS;
        String tblName = idxName + bucket;
        ts = new TableScan(tx, tblName, layout);
    }

    @Override
    public boolean next() {
        while (ts.next())
            if (ts.getVal("dataVal").equals(searchKey))
                return true;
        return false;
    }

    @Override
    public RID getDataRid() {
        int blkNum = ts.getInt("block");
        int id = ts.getInt("id");
        return new RID(blkNum, id);
    }

    @Override
    public void insert(Constant val, RID rid) {
        beforeFirst(val);
        ;
        ts.insert();
        ts.setInt("block", rid.blockNumber());
        ts.setInt("id", rid.slot());
        ts.setVal("dataVal", val);
    }

    @Override
    public void delete(Constant val, RID rid) {
        beforeFirst(val);
        while (next())
            if (getDataRid().equals(rid)) {
                ts.delete();
                return;
            }
    }

    @Override
    public void close() {
        if (ts != null)
            ts.close();
    }

    public static int searchCost(int numBlocks, int rpb) {
        return numBlocks / HashIndex.NUM_BUCKETS;
    }
}

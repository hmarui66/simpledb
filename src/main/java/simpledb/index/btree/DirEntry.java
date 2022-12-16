package simpledb.index.btree;

import simpledb.query.Constant;

public class DirEntry {
    private Constant dataVal;
    private int blkNum;
    public DirEntry(Constant dataVal, int blockNum) {
        this.dataVal = dataVal;
        this.blkNum = blockNum;
    }

    public Constant dataVal() {
        return dataVal;
    }

    public int blockNumber() {
        return blkNum;
    }
}

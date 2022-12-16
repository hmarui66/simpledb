package simpledb.multibuffer;

import simpledb.query.Constant;
import simpledb.query.ProductScan;
import simpledb.query.Scan;
import simpledb.record.Layout;
import simpledb.tx.Transaction;

public class MultiBufferProductScan implements Scan {
    private Transaction tx;
    private Scan lhsScan, rhsScan = null, prodScan;
    private String fileName;
    private Layout layout;
    private int chunkSize, nextBlkNum, fileSize;
    public MultiBufferProductScan(Transaction tx, Scan lhsScan, String tblName, Layout layout) {
        this.tx = tx;
        this.lhsScan = lhsScan;
        this.fileName = tblName + ".tbl";
        this.layout = layout;
        fileSize = tx.size(fileName);
        int available = tx.availableBuffs();
        chunkSize = BufferNeeds.bestFactor(available, fileSize);
        beforeFirst();
    }

    @Override
    public void beforeFirst() {
        nextBlkNum = 0;
        useNextScan();
    }

    public boolean next() {
        while (!prodScan.next()) {
            if (!useNextScan()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void close() {
        prodScan.close();
    }

    @Override
    public Constant getVal(String fieldName) {
        return prodScan.getVal(fieldName);
    }

    @Override
    public int getInt(String fieldName) {
        return prodScan.getInt(fieldName);
    }

    @Override
    public String getString(String fieldName) {
        return prodScan.getString(fieldName);
    }

    @Override
    public boolean hasField(String fieldName) {
        return prodScan.hasField(fieldName);
    }

    private boolean useNextScan() {
        if (nextBlkNum >= fileSize) {
            return false;
        }
        if (rhsScan != null) {
            rhsScan.close();
        }
        int end = nextBlkNum + chunkSize - 1;
        if (end >= fileSize) {
            end = fileSize - 1;
        }
        rhsScan = new ChunkScan(tx, fileName, layout, nextBlkNum, end);
        lhsScan.beforeFirst();
        prodScan = new ProductScan(lhsScan, rhsScan);
        nextBlkNum = end + 1;
        return true;
    }
}

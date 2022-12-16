package simpledb.index.query;

import simpledb.index.Index;
import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.record.RID;
import simpledb.record.TableScan;

public class IndexSelectScan implements Scan {
    private TableScan ts;
    private Index idx;
    private Constant val;
    public IndexSelectScan(TableScan ts, Index idx, Constant val) {
        this.ts = ts;
        this.idx = idx;
        this.val = val;
        beforeFirst();
    }

    /**
     * Positions the scan before the first record,
     * which in this case means positioning the index
     * before the first instance of the selection constant.
     */
    public void beforeFirst() {
        idx.beforeFirst(val);
    }

    /**
     * Moves to the next record, which in this case means
     * moving the index to the next record satisfying the
     * selection constant, and returning false if there are
     * no more such index records.
     * If there is a next record, the method moves the
     * tableScan to the corresponding data record.
     */
    public boolean next() {
        boolean ok = idx.next();
        if (ok) {
            RID rid = idx.getDataRid();
            ts.moveToRid(rid);
        }
        return ok;
    }

    /**
     * Returns the value of the field of the current data record.
     */
    public int getInt(String fieldName) {
        return ts.getInt(fieldName);
    }

    /**
     * Returns the value of the field of the current data record.
     */
    public String getString(String fieldName) {
        return ts.getString(fieldName);
    }

    /**
     * Returns the value of the field of the current data record.
     */
    public Constant getVal(String fieldName) {
        return ts.getVal(fieldName);
    }

    /**
     * Returns whether the data record has the specified field.
     */
    public boolean hasField(String fieldName) {
        return ts.hasField(fieldName);
    }

    /**
     * Closes the scan by closing the index and the tableScan
     */
    public void close() {
        idx.close();
        ts.close();
    }

}

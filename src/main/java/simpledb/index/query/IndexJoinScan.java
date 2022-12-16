package simpledb.index.query;

import simpledb.index.Index;
import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.record.TableScan;

public class IndexJoinScan implements Scan {
    private Scan lhs;
    private Index idx;
    private String joinField;
    private TableScan rhs;
    public IndexJoinScan(Scan lhs, Index idx, String joinField, TableScan rhs) {
        this.lhs = lhs;
        this.idx = idx;
        this.joinField = joinField;
        this.rhs = rhs;
        beforeFirst();
    }

    /**
     * Positions the scan before the first record.
     * That is, the LHS scan will be positioned at its
     * first record, and the index will be positioned
     * before the first record for the join value.
     */
    public void beforeFirst() {
        lhs.beforeFirst();
        lhs.next();
        resetIndex();
    }

    /**
     * Moves the scan to the next record.
     * The method moves to the next index record, if possible.
     * Otherwise, it moves to the next LHS record and the
     * first index record.
     * If there are no more LHS records, the method returns false.
     */
    public boolean next() {
        while (true) {
            if (idx.next()) {
                rhs.moveToRid(idx.getDataRid());
                return true;
            }
            if (!lhs.next())
                return false;
            resetIndex();
        }
    }

    /**
     * Returns the integer value of the specified field.
     */
    public int getInt(String fieldName) {
        if (rhs.hasField(fieldName))
            return rhs.getInt(fieldName);
        else
            return lhs.getInt(fieldName);
    }

    /**
     * Returns the Constant value of the specified field.
     */
    public Constant getVal(String fieldName) {
        if (rhs.hasField(fieldName))
            return rhs.getVal(fieldName);
        else
            return lhs.getVal(fieldName);
    }

    /**
     * Returns the String value of the specified field.
     */
    public String getString(String fieldName) {
        if (rhs.hasField(fieldName))
            return rhs.getString(fieldName);
        else
            return lhs.getString(fieldName);
    }

    /**
     * Returns true if the field is in the schema.
     */
    public boolean hasField(String fieldName) {
        return rhs.hasField(fieldName) || lhs.hasField(fieldName);
    }

    /**
     * Closes the scan by closing its LHS scan and RHS scan and index.
     */
    public void close() {
        lhs.close();
        idx.close();
        rhs.close();
    }

    private void resetIndex() {
        Constant searchKey = lhs.getVal(joinField);
        idx.beforeFirst(searchKey);
    }
}

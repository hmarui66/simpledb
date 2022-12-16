package simpledb.materizlize;

import simpledb.query.Constant;
import simpledb.query.Scan;

public class MergeJoinScan implements Scan {
    private Scan s1;
    private SortScan s2;
    private String fieldName1, fieldName2;
    private Constant joinVal = null;

    public MergeJoinScan(Scan s1, SortScan s2, String fieldName1, String fieldName2) {
        this.s1 = s1;
        this.s2 = s2;
        this.fieldName1 = fieldName1;
        this.fieldName2 = fieldName2;
        beforeFirst();
    }

    @Override
    public void close() {
        s1.close();
        s2.close();
    }

    @Override
    public void beforeFirst() {
        s1.beforeFirst();
        s2.beforeFirst();
    }

    /**
     * Move to the next record. This is where the action is.
     * If the next RHS record has the same join value,
     * then move to it.
     * Otherwise, if the next LHS record has the same join value,
     * then reposition the RHS scan back to the first record
     * having that join value.
     * Otherwise, repeatedly move the scan having the smallest
     * value until a common join value is found.
     * When one of the scans runs out of records, return false.
     */
    @Override
    public boolean next() {
        boolean hasMore2 = s2.next();
        if (hasMore2 && s2.getVal(fieldName2).equals(joinVal)) {
            return true;
        }

        boolean hasMore1 = s1.next();
        if (hasMore1 && s1.getVal(fieldName1).equals(joinVal)) {
            s2.restorePosition();
            return true;
        }

        while (hasMore1 && hasMore2) {
            Constant v1 = s1.getVal(fieldName1);
            Constant v2 = s2.getVal(fieldName2);
            if (v1.compareTo(v2) < 0) {
                hasMore1 = s1.next();
            } else if (v1.compareTo(v2) > 0) {
                hasMore2 = s2.next();
            } else {
                s2.savePosition();
                joinVal = s2.getVal(fieldName2);
                return true;
            }
        }
        return false;
    }

    @Override
    public int getInt(String fieldName) {
        if (s1.hasField(fieldName)) {
            return s1.getInt(fieldName);
        } else {
            return s2.getInt(fieldName);
        }
    }

    @Override
    public String getString(String fieldName) {
        if (s1.hasField(fieldName)) {
            return s1.getString(fieldName);
        } else {
            return s2.getString(fieldName);
        }
    }
    @Override
    public Constant getVal(String fieldName) {
        if (s1.hasField(fieldName)) {
            return s1.getVal(fieldName);
        } else {
            return s2.getVal(fieldName);
        }
    }

    @Override
    public boolean hasField(String fieldName) {
        return s1.hasField(fieldName) || s2.hasField(fieldName);
    }
}

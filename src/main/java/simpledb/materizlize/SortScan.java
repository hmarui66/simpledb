package simpledb.materizlize;

import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.RID;

import java.util.Arrays;
import java.util.List;

public class SortScan implements Scan {
    private UpdateScan s1, s2 = null, currentScan = null;
    private RecordComparator comp;
    private boolean hasMore1, hasMore2 = false;
    private List<RID> savedPosition;

    public SortScan(List<TempTable> runs, RecordComparator comp) {
        this.comp = comp;
        s1 = (UpdateScan) runs.get(0).open();
        hasMore1 = s1.next();
        if (runs.size() > 1) {
            s2 = (UpdateScan) runs.get(1).open();
            hasMore2 = s2.next();
        }
    }

    /**
     * Position the scan before the first record in sorted order.
     * Internally, it moves to the first record of each underlying scan.
     * The variable currentScan is set to null, indicating that there is
     * no current scan.
     */
    @Override
    public void beforeFirst() {
        currentScan = null;
        s1.beforeFirst();
        hasMore1 = s1.next();
        if (s2 != null) {
            s2.beforeFirst();
            hasMore2 = s2.next();
        }
    }

    /**
     * Move to the next record in sorted order.
     * First, the current scan is moved to the next record.
     * Then the lowest record of the two scans is found, and that
     * scan is chosen to be the new current scan.
     */
    @Override
    public boolean next() {
        if (currentScan != null) {
            if (currentScan == s1)
                hasMore1 = s1.next();
            else if (currentScan == s2)
                hasMore2 = s2.next();
        }

        if (!hasMore1 && !hasMore2)
            return false;
        else if (hasMore1 && hasMore2) {
            if (comp.compare(s1, s2) < 0)
                currentScan = s1;
            else
                currentScan = s2;
        } else if (hasMore1) {
            currentScan = s1;
        } else if (hasMore2)
            currentScan = s2;
        return true;
    }

    @Override
    public void close() {
        s1.close();
        if (s2 != null)
            s2.close();
    }

    /**
     * Get the Constant value of the specified field
     * of the current scan.
     */
    @Override
    public Constant getVal(String fieldName) {
        return currentScan.getVal(fieldName);
    }

    /**
     * Get the integer value of the specified field
     * of the current scan.
     */
    @Override
    public int getInt(String fieldName) {
        return currentScan.getInt(fieldName);
    }

    /**
     * Get the string value of the specified field
     * of the current scan.
     */
    @Override
    public String getString(String fieldName) {
        return null;
    }

    /**
     * Return true if the specified field in the current scan
     */
    @Override
    public boolean hasField(String fieldName) {
        return currentScan.hasField(fieldName);
    }

    /**
     * Save the position of the current record,
     * so that it can be restored at a later time.
     */
    public void savePosition() {
        RID rid1 = s1.getRid();
        RID rid2 = (s2 == null) ? null : s2.getRid();
        savedPosition = Arrays.asList(rid1, rid2);
    }

    /**
     * Move the scan to its previously-saved position.
     */
    public void restorePosition() {
        RID rid1 = savedPosition.get(0);
        RID rid2 = savedPosition.get(1);
        s1.moveToRid(rid1);
        if (rid2 != null)
            s2.moveToRid(rid2);
    }
}

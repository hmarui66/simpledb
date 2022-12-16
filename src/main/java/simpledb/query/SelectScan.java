package simpledb.query;

import simpledb.record.RID;

public class SelectScan implements UpdateScan {
    private Scan s;
    private Predicate pred;

    public SelectScan(Scan s, Predicate pred) {
        this.s = s;
        this.pred = pred;
    }

    // Scan methods

    public void beforeFirst() {
        s.beforeFirst();
    }

    public boolean next() {
        while (s.next())
            if (pred.isSatisfied(s))
                return true;
        return false;
    }

    public int getInt(String fieldName) {
        return s.getInt(fieldName);
    }

    public String getString(String fieldName) {
        return s.getString(fieldName);
    }

    public boolean hasField(String fieldName) {
        return s.hasField(fieldName);
    }

    public void close() {
        s.close();
    }

    @Override
    public Constant getVal(String fieldName) {
        return s.getVal(fieldName);
    }

    // UpdateScan methods

    @Override
    public void setVal(String fieldName, Constant val) {
        UpdateScan us = (UpdateScan) s;
        us.setVal(fieldName, val);
    }

    @Override
    public void setInt(String fieldName, int val) {
        UpdateScan us = (UpdateScan) s;
        us.setInt(fieldName, val);
    }

    @Override
    public void setString(String fieldName, String val) {
        UpdateScan us = (UpdateScan) s;
        us.setString(fieldName, val);
    }

    @Override
    public void insert() {
        UpdateScan us = (UpdateScan) s;
        us.insert();
    }

    @Override
    public void delete() {
        UpdateScan us = (UpdateScan) s;
        us.delete();
    }

    @Override
    public RID getRid() {
        UpdateScan us = (UpdateScan) s;
        return us.getRid();
    }

    @Override
    public void moveToRid(RID rid) {
        UpdateScan us = (UpdateScan) s;
        us.moveToRid(rid);
    }
}

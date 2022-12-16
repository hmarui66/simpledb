package simpledb.query;

import simpledb.record.RID;

public interface UpdateScan extends Scan {

    void setVal(String fieldName, Constant val);
    public void setInt(String fieldName, int val);
    public void setString(String fieldName, String val);
    public void insert();
    public void delete();

    public RID getRid();
    public void moveToRid(RID rid);
}

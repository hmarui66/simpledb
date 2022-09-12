package query;

import record.RID;

public interface UpdateScan {
    public void setInt(String fieldName, int val);
    public void setString(String fieldName, String val);
    public void insert();
    public void delete();

    public RID getRid();
    public void moveToRid(RID rid);
}

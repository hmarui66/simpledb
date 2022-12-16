package simpledb.query;

public interface Scan {
    void beforeFirst();
    public boolean next();
    public int getInt(String fieldName);
    public String getString(String fieldName);
    public boolean hasField(String fieldName);
    public void close();
    Constant getVal(String fieldName);
}

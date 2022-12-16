package simpledb.query;

public class ProductScan implements Scan {

    private Scan s1, s2;

    public ProductScan(Scan s1, Scan s2) {
        this.s1 = s1;
        this.s2 = s2;
        beforeFirst();
    }

    @Override
    public void beforeFirst() {
        s1.beforeFirst();
        s1.next();
        s2.beforeFirst();
    }

    @Override
    public boolean next() {
        if (s2.next())
            return true;
        else {
            s2.beforeFirst();
            return s2.next() && s1.next();
        }
    }

    @Override
    public int getInt(String fieldName) {
        if (s1.hasField(fieldName))
            return s1.getInt(fieldName);
        else
            return s2.getInt(fieldName);
    }

    @Override
    public String getString(String fieldName) {
        if (s1.hasField(fieldName))
            return s1.getString(fieldName);
        else
            return s2.getString(fieldName);
    }

    @Override
    public Constant getVal(String fieldName) {
        if (s1.hasField(fieldName))
            return s1.getVal(fieldName);
        else
            return s2.getVal(fieldName);
    }

    @Override
    public boolean hasField(String fieldName) {
        return s1.hasField(fieldName) || s2.hasField(fieldName);
    }

    @Override
    public void close() {
        s1.close();
        s2.close();
    }
}

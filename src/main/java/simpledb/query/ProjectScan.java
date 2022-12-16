package simpledb.query;

import java.util.List;

public class ProjectScan implements Scan {
    private Scan s;
    private List<String> fieldList;

    public ProjectScan(Scan s, List<String> fieldList) {
        this.s = s;
        this.fieldList = fieldList;
    }

    @Override
    public void beforeFirst() {
        s.beforeFirst();
    }

    @Override
    public boolean next() {
        return s.next();
    }

    @Override
    public int getInt(String fieldName) {
        if (hasField(fieldName))
            return s.getInt(fieldName);

        throw new RuntimeException("field " + fieldName + " not found.");
    }

    @Override
    public String getString(String fieldName) {
        if (hasField(fieldName))
            return s.getString(fieldName);

        throw new RuntimeException("field " + fieldName + " not found.");
    }

    @Override
    public Constant getVal(String fieldName) {
        if (hasField(fieldName))
            return s.getVal(fieldName);

        throw new RuntimeException("field " + fieldName + " not found.");
    }

    @Override
    public boolean hasField(String fieldName) {
        return fieldList.contains(fieldName);
    }

    @Override
    public void close() {
        s.close();
    }

}

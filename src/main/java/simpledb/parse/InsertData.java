package simpledb.parse;

import simpledb.query.Constant;

import java.util.List;

public class InsertData {
    private String tblName;
    private List<String> fields;
    private List<Constant> vals;
    public InsertData(String tblName, List<String> fields, List<Constant> vals) {
        this.tblName = tblName;
        this.fields = fields;
        this.vals = vals;
    }

    public String tableName() {
        return tblName;
    }

    public List<String> fields() {
        return fields;
    }

    public List<Constant> vals() {
        return vals;
    }
}

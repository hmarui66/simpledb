package simpledb.parse;

import simpledb.record.Schema;

public class CreateTableData {
    private String tblName;
    private Schema sch;

    public CreateTableData(String tblName, Schema sch) {
        this.tblName = tblName;
        this.sch = sch;
    }

    public String tableName() {
        return tblName;
    }

    public Schema schema() {
        return sch;
    }

    public Schema newSchema() {
        return sch;
    }
}

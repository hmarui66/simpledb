package parse;

import record.Schema;

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
}
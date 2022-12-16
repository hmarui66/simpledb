package simpledb.parse;

public class CreateIndexData {
    private String idxName, tblName, fieldName;
    public CreateIndexData(String idxName, String tblName, String fieldName) {
        this.idxName = idxName;
        this.tblName = tblName;
        this.fieldName = fieldName;
    }

    public String idxName() {
        return idxName;
    }

    public String tableName() {
        return tblName;
    }

    public String fieldName() {
        return fieldName;
    }

    public String indexName() {
        return idxName;
    }
}

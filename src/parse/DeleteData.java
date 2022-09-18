package parse;

import query.Predicate;

public class DeleteData {
    private String tblName;
    private Predicate pred;

    public DeleteData(String tblName, Predicate pred) {
        this.tblName = tblName;
        this.pred = pred;
    }

    public String tableName() {
        return tblName;
    }
    public Predicate pred() {
        return pred;
    }
}

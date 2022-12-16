package simpledb.parse;

import simpledb.query.Expression;
import simpledb.query.Predicate;

public class ModifyData {
    private String tblName;
    private String fieldName;
    private Expression newVal;
    private Predicate pred;

    public ModifyData(String tblName, String fieldName, Expression newVal, Predicate pred) {
        this.tblName = tblName;
        this.fieldName = fieldName;
        this.newVal = newVal;
        this.pred = pred;
    }

    public String tableName() {
        return tblName;
    }

    public String targetField() {
        return fieldName;
    }

    public Expression newVal() {
        return newVal;
    }

    public Predicate pred() {
        return pred;
    }
}

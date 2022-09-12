package query;

import record.Schema;

public class Expression {
    private Constant val = null;
    private String fieldName = null;

    public Expression(Constant val) {
        this.val = val;
    }

    public Expression(String fieldName ) {
        this.fieldName = fieldName;
    }

    public boolean isFieldName() {
        return fieldName != null;
    }

    public Constant asConstant() {
        return val;
    }

    public String asFieldName() {
        return fieldName;
    }

    public Constant evaluate(Scan s) {
        return (val != null) ? val : s.getVal(fieldName);
    }

    public boolean appliesTo(Schema sch) {
        return val != null || sch.hasField(fieldName);
    }

    public String toString() {
        return (val != null ) ? val.toString() : fieldName;
    }
}

package simpledb.materizlize;

import simpledb.query.Constant;
import simpledb.query.Scan;

public class MaxFn implements AggregateFn {
    private String fieldName;
    private Constant val;

    public MaxFn(String fieldName) {
        this.fieldName = fieldName;
    }

    public void processFirst(Scan s) {
        val = s.getVal(fieldName);
    }

    public void processNext(Scan s) {
        Constant newVal = s.getVal(fieldName);
        if (newVal.compareTo(val) > 0) {
            val = newVal;
        }
    }

    public String fieldName() {
        return "maxOf" + fieldName;
    }

    public Constant value() {
        return val;
    }
}

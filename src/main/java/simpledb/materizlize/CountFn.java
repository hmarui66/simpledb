package simpledb.materizlize;

import simpledb.query.Constant;
import simpledb.query.Scan;

public class CountFn implements AggregateFn {
    private String fieldName;
    private int count;

    public CountFn(String fieldName) {
        this.fieldName = fieldName;
    }

    public void processFirst(Scan s) {
        count = 1;
    }

    public void processNext(Scan s) {
        count++;
    }

    public String fieldName() {
        return "countOf" + fieldName;
    }

    public Constant value() {
        return new Constant(count);
    }
}

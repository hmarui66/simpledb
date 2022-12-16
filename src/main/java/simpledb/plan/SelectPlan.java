package simpledb.plan;

import simpledb.query.Predicate;
import simpledb.query.Scan;
import simpledb.query.SelectScan;
import simpledb.record.Schema;

public class SelectPlan implements Plan {
    private Plan p;
    private Predicate pred;

    public SelectPlan(Plan p, Predicate pred) {
        this.p = p;
        this.pred = pred;
    }

    @Override
    public Scan open() {
        Scan s = p.open();
        return new SelectScan(s, pred);
    }

    @Override
    public int blockAccessed() {
        return p.blockAccessed();
    }

    @Override
    public int recordsOutput() {
        return p.recordsOutput() / pred.reductionFactor(p);
    }

    @Override
    public int distinctValues(String fieldName) {
        if (pred.equatesWithConstant(fieldName) != null)
            return 1;
        else {
            String fieldName2 = pred.equatesWithField(fieldName);
            if (fieldName2 != null)
                return Math.min(
                        p.distinctValues(fieldName),
                        p.distinctValues(fieldName2)
                );
            else
                return p.distinctValues(fieldName);
        }
    }

    @Override
    public Schema schema() {
        return p.schema();
    }
}

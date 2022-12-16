package simpledb.materizlize;

import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

import java.util.ArrayList;
import java.util.List;

public class SortPlan implements Plan {
    private Transaction tx;
    private Plan p;
    private Schema sch;
    private RecordComparator comp;

    public SortPlan(Transaction tx, Plan p, List<String> sortFields) {
        this.tx = tx;
        this.p = p;
        sch = p.schema();
        comp = new RecordComparator(sortFields);
    }

    /**
     * This method is where most of the action is.
     * Up to 2 sorted temporary tables are created,
     * and are passed into SortScan for final merging.
     */
    @Override
    public Scan open() {
        Scan src = p.open();
        List<TempTable> runs = splintIntoRuns(src);
        src.close();
        while (runs.size() > 2)
            runs = doAMergeIteration(runs);
        return new SortScan(runs, comp);
    }

    /**
     * Returns the number of blocks in the sorted table,
     * which is the same as it would be in a
     * meterizlized table.
     * It does not include the one-time cost
     * of materializing and sorting the records.
     */
    @Override
    public int blockAccessed() {
        // onetime のソートコストは含まない
        Plan mp = new MaterializePlan(tx, p); // open しない; 分析のみ
        return mp.blockAccessed();
    }

    /**
     * Return the number of records in the sorted table,
     * which is the same as in the underlying query.
     */
    @Override
    public int recordsOutput() {
        return p.recordsOutput();
    }

    /**
     * Return the number of distinct field values in
     * the sorted table, which is the same as in
     * the underlying query.
     */
    @Override
    public int distinctValues(String fieldName) {
        return p.distinctValues(fieldName);
    }

    /**
     * Return the schema of the sorted table, which
     * is the same as in the underlying query.
     */
    @Override
    public Schema schema() {
        return sch;
    }

    private List<TempTable> splintIntoRuns(Scan src) {
        List<TempTable> temps = new ArrayList<>();
        src.beforeFirst();
        if (!src.next())
            return temps;
        TempTable currentTemp = new TempTable(tx, sch);
        temps.add(currentTemp);
        UpdateScan currentScan = currentTemp.open();
        while (copy(src, currentScan))
            if (comp.compare(src, currentScan) < 0) {
                // start a new run
                currentScan.close();
                currentTemp = new TempTable(tx, sch);
                temps.add(currentTemp);
                currentScan = (UpdateScan) currentTemp.open();
            }
        currentScan.close();
        return temps;
    }

    private List<TempTable> doAMergeIteration(List<TempTable> runs) {
        List<TempTable> result = new ArrayList<>();
        while (runs.size() > 1) {
            TempTable p1 = runs.remove(0);
            TempTable p2 = runs.remove(0);
            result.add(mergeTwoRuns(p1, p2));
        }
        if (runs.size() == 1)
            result.add(runs.get(0));
        return result;
    }

    private TempTable mergeTwoRuns(TempTable p1, TempTable p2) {
        Scan src1 = p1.open();
        Scan src2 = p2.open();
        TempTable result = new TempTable(tx, sch);
        UpdateScan dest = result.open();

        boolean hasMore1 = src1.next();
        boolean hasMore2 = src2.next();
        while (hasMore1 && hasMore2) {
            if (comp.compare(src1, src2) < 0)
                hasMore1 = copy(src1, dest);
            else
                hasMore2 = copy(src2, dest);
        }

        if (hasMore1) {
            while (hasMore1)
                hasMore1 = copy(src1, dest);
        } else {
            while (hasMore2)
                hasMore2 = copy(src2, dest);
        }

        src1.close();
        src2.close();
        dest.close();
        return result;
    }

    private boolean copy(Scan src, UpdateScan dest) {
        dest.insert();
        for (String fieldName : sch.fields()) {
            dest.setVal(fieldName, src.getVal(fieldName));
            ;
        }
        return src.next();
    }
}

package simpledb.plan;

import simpledb.metadata.MetadataMgr;
import simpledb.metadata.StatInfo;
import simpledb.query.Scan;
import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

public class TablePlan implements Plan {

    private String tblName;
    private Transaction tx;
    private Layout layout;
    private StatInfo si;

    public TablePlan(Transaction tx, String tblName, MetadataMgr md) {
        this.tblName = tblName;
        this.tx = tx;
        layout = md.getLayout(tblName, tx);
        si = md.getStatInfo(tblName, layout, tx);
    }

    @Override
    public Scan open() {
        return new TableScan(tx, tblName, layout);
    }

    @Override
    public int blockAccessed() {
        return si.blocksAccessed();
    }

    @Override
    public int recordsOutput() {
        return si.recordsOutput();
    }

    @Override
    public int distinctValues(String fieldName) {
        return si.distinctValues(fieldName);
    }

    @Override
    public Schema schema() {
        return layout.schema();
    }
}

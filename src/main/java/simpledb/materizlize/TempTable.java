package simpledb.materizlize;

import simpledb.query.UpdateScan;
import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

public class TempTable {
    private static int nextTableNum = 0;
    private Transaction tx;
    private String tableName;
    private Layout layout;

    /**
     * Allocate a name for a new temporary table
     * having the specified schema.
     */
    public TempTable(Transaction tx, Schema sch) {
        this.tx = tx;
        tableName = nextTableName();
        layout = new Layout(sch);
    }

    /**
     * Open a table scan for the temporary table.
     */
    public UpdateScan open() {
        return new TableScan(tx, tableName, layout);
    }

    public String tableName() {
        return tableName;
    }

    /**
     * Return the table's metadata.
     */
    public Layout getLayout() {
        return layout;
    }

    private static synchronized String nextTableName() {
        nextTableNum++;
        return "temp" + nextTableNum;
    }
}

package simpledb.metadata;

import simpledb.record.Layout;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

import java.util.HashMap;
import java.util.Map;

public class StatMgr {
    private TableMgr tblMgr;
    private Map<String, StatInfo> tableStats;
    private int numCalls;

    public StatMgr(TableMgr tblMgr, Transaction tx) {
        this.tblMgr = tblMgr;
        refreshStatistics(tx);
    }

    public synchronized StatInfo getStatInfo(String tableName, Layout layout, Transaction tx) {
        numCalls++;
        if (numCalls > 100)
            refreshStatistics(tx);
        StatInfo si = tableStats.get(tableName);
        if (si == null) {
            si = calcTableStats(tableName, layout, tx);
            tableStats.put(tableName, si);
        }
        return si;
    }

    private synchronized void refreshStatistics(Transaction tx) {
        tableStats = new HashMap<>();
        numCalls = 0;
        Layout tableCatalogLayout = tblMgr.getLayout("tableCatalog", tx);
        TableScan ts = new TableScan(tx, "tableCatalog", tableCatalogLayout);
        while (ts.next()) {
            String tableName = ts.getString("tableName");
            Layout layout = tblMgr.getLayout(tableName, tx);
            StatInfo si = calcTableStats(tableName, layout, tx);
            tableStats.put(tableName, si);
        }
        ts.close();
    }

    private synchronized StatInfo calcTableStats(String tableName, Layout layout, Transaction tx) {
        int numRecs = 0;
        int numBlocks = 0;
        TableScan ts = new TableScan(tx, tableName, layout);
        while (ts.next()) {
            numRecs++;
            numBlocks = ts.getRid().blockNumber() + 1;
        }
        ts.close();
        return new StatInfo(numBlocks, numRecs);
    }
}

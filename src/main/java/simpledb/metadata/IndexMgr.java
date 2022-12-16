package simpledb.metadata;

import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

import java.util.HashMap;
import java.util.Map;

public class IndexMgr {
    private Layout layout;
    private TableMgr tblMgr;
    private StatMgr statMgr;

    public IndexMgr(boolean isNew, TableMgr tblMgr, StatMgr statMgr, Transaction tx) {
        if (isNew) {
            Schema sch = new Schema();
            sch.addStringField("indexName", TableMgr.MAX_NAME);
            sch.addStringField("tableName", TableMgr.MAX_NAME);
            sch.addStringField("fieldName", TableMgr.MAX_NAME);
            tblMgr.createTable("idxCatalog", sch, tx);
        }
        this.tblMgr = tblMgr;
        this.statMgr = statMgr;
        layout = tblMgr.getLayout("idxCatalog", tx);
    }

    public void createIndex(String idxName, String tblName, String fieldName, Transaction tx) {
        TableScan ts = new TableScan(tx, "idxCatalog", layout);
        ts.insert();
        ts.setString("indexName", idxName);
        ts.setString("tableName", tblName);
        ts.setString("fieldName", fieldName);
        ts.close();
    }

    public Map<String, IndexInfo> getIndexInfo(String tblName, Transaction tx) {
        Map<String, IndexInfo> result = new HashMap<>();
        TableScan ts = new TableScan(tx, "idxCatalog", layout);
        while (ts.next()) {
            if (ts.getString("tableName").equals(tblName)) {
                String idxName = ts.getString("indexName");
                String fieldName = ts.getString("fieldName");
                Layout tblLayout = tblMgr.getLayout(tblName, tx);
                StatInfo si = statMgr.getStatInfo(tblName, tblLayout, tx);
                IndexInfo ii = new IndexInfo(idxName, fieldName, tblLayout.schema(), tx, si);
                result.put(fieldName, ii);
            }
        }
        ts.close();
        return result;
    }
}

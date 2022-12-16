package simpledb.metadata;

import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

import java.util.Map;

public class MetadataMgr {
    private static TableMgr tblMgr;
    private static ViewMgr viewMgr;
    private static StatMgr statMgr;
    private static IndexMgr idxMgr;

    public MetadataMgr(boolean isNew, Transaction tx) {
        tblMgr = new TableMgr(isNew, tx);
        viewMgr = new ViewMgr(isNew, tblMgr, tx);
        statMgr = new StatMgr(tblMgr, tx);
        idxMgr = new IndexMgr(isNew, tblMgr, statMgr, tx);
    }

    public void createTable(String tblName, Schema sch, Transaction tx) {
        tblMgr.createTable(tblName, sch, tx);
    }

    public Layout getLayout(String tblName, Transaction tx) {
        return tblMgr.getLayout(tblName, tx);
    }

    public void createView(String viewName, String viewDef, Transaction tx) {
        viewMgr.createView(viewName, viewDef, tx);
    }

    public String getViewDef(String viewName, Transaction tx) {
        return viewMgr.getViewDef(viewName, tx);
    }

    public void createIndex(String idxName, String tblName, String fieldName, Transaction tx) {
        idxMgr.createIndex(idxName, tblName, fieldName, tx);
    }

    public Map<String, IndexInfo> getIndexInfo(String tblName, Transaction tx) {
        return idxMgr.getIndexInfo(tblName, tx);
    }

    public StatInfo getStatInfo(String tblName, Layout layout, Transaction tx) {
        return statMgr.getStatInfo(tblName, layout, tx);
    }
}

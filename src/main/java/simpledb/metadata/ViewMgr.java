package simpledb.metadata;

import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

public class ViewMgr {
    private static final int MAX_VIEWDEF = 100;

    TableMgr tblMgr;

    public ViewMgr(boolean isNew, TableMgr tblMgr, Transaction tx) {
        this.tblMgr = tblMgr;
        if (isNew) {
            Schema sch = new Schema();
            sch.addStringField("viewName", TableMgr.MAX_NAME);
            sch.addStringField("viewDef", MAX_VIEWDEF);
            tblMgr.createTable("viewCatalog", sch, tx);
        }
    }

    public void createView(String viewName, String viewDef, Transaction tx) {
        Layout layout = tblMgr.getLayout("viewCatalog", tx);
        TableScan ts = new TableScan(tx, "viewCatalog", layout);
        ts.insert();
        ts.setString("viewName", viewName);
        ts.setString("viewDef", viewDef);
        ts.close();
    }

    public String getViewDef(String viewName, Transaction tx) {
        String result = null;
        Layout layout = tblMgr.getLayout("viewCatalog", tx);
        TableScan ts = new TableScan(tx, "viewCatalog", layout);
        while (ts.next()) {
            if (ts.getString("viewName").equals(viewName)) {
                result = ts.getString("viewDef");
                break;
            }
        }
        ts.close();

        return result;
    }
}

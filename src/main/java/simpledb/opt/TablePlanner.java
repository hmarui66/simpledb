package simpledb.opt;

import simpledb.index.planner.IndexJoinPlan;
import simpledb.index.planner.IndexSelectPlan;
import simpledb.metadata.IndexInfo;
import simpledb.metadata.MetadataMgr;
import simpledb.multibuffer.MultiBufferProductPlan;
import simpledb.plan.Plan;
import simpledb.plan.SelectPlan;
import simpledb.plan.TablePlan;
import simpledb.query.Constant;
import simpledb.query.Predicate;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

import java.util.Map;

public class TablePlanner {
    private TablePlan myPlan;
    private Predicate myPred;
    private Schema mySchema;
    private Map<String, IndexInfo> indexes;
    private Transaction tx;

    public TablePlanner(String tblName, Predicate myPred, Transaction tx, MetadataMgr mdm) {
        this.myPred = myPred;
        this.tx = tx;
        myPlan = new TablePlan(tx, tblName, mdm);
        mySchema = myPlan.schema();
        indexes = mdm.getIndexInfo(tblName, tx);
    }

    /**
     * Constructs a select plan for the table.
     * The plan will use an IndexSelect, if possible.
     */
    public Plan makeSelectPlan() {
        Plan p = makeIndexSelect();
        if (p == null) {
            p = myPlan;
        }
        return addSelectPred(p);
    }

    /**
     * Constructs a join plan of the specified plan
     * and the table. The plan will use an IndexJoin, if possible.
     * (Which means that if an IndexSelect is also possible,
     * the IndexJoin operator takes precedence.)
     * The method returns null if no join is possible.
     */
    public Plan makeJoinPlan(Plan current) {
        Schema currentSch = current.schema();
        // 自身のスキーマと join 対象のスキーマに関する predicate があれば取得
        Predicate joinPred = myPred.joinSubPred(mySchema, currentSch);
        if (joinPred == null) {
            return null;
        }
        Plan p = makeIndexJoin(current, currentSch);
        if (p == null) {
            p = makeProductJoin(current, currentSch);
        }
        return p;
    }

    /**
     * Constructs a product plan of the specified plan and
     * this table.
     */
    public Plan makeProductPlan(Plan current) {
        Plan p = addSelectPred(myPlan);
        return new MultiBufferProductPlan(tx, current, p);
    }

    private Plan makeIndexSelect() {
        for (String fieldName : indexes.keySet()) {
            Constant val = myPred.equatesWithConstant(fieldName);
            if (val != null) {
                IndexInfo ii = indexes.get(fieldName);
                System.out.println("index on " + fieldName + " used");
                return new IndexSelectPlan(myPlan, ii, val);
            }
        }
        return null;
    }

    private Plan makeIndexJoin(Plan current, Schema currentSch) {
        for (String fieldName : indexes.keySet()) {
            String outerField = myPred.equatesWithField(fieldName);
            if (outerField != null && currentSch.hasField(outerField)) {
                IndexInfo ii = indexes.get(fieldName);
                Plan p = new IndexJoinPlan(current, myPlan, ii, outerField);
                p = addSelectPred(p);
                return addJoinPred(p, currentSch);
            }
        }
        return null;
    }

    private Plan makeProductJoin(Plan current, Schema currentSch) {
        Plan p = makeProductPlan(current);
        return addJoinPred(p, currentSch);
    }

    private Plan addSelectPred(Plan p) {
        Predicate selectPred = myPred.selectSubPred(mySchema);
        if (selectPred != null) {
            return new SelectPlan(p, selectPred);
        } else {
            return p;
        }
    }

    private Plan addJoinPred(Plan p, Schema currentSch) {
        Predicate joinPred = myPred.joinSubPred(currentSch, mySchema);
        if (joinPred != null) {
            return new SelectPlan(p, joinPred);
        } else {
            return p;
        }
    }


}

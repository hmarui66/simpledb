package simpledb.materizlize;

import simpledb.query.Constant;
import simpledb.query.Scan;

import java.util.List;

public class GroupByScan implements Scan {
    private Scan s;
    private List<String> groupFields;
    private List<AggregateFn> aggFns;
    private GroupValue groupVal;
    private boolean moreGroups;

    public GroupByScan(Scan s, List<String> groupFields, List<AggregateFn> aggFns) {
        this.s = s;
        this.groupFields = groupFields;
        this.aggFns = aggFns;
        beforeFirst();
    }

    /**
     * Position the scan before the first group.
     * Internally, the underlying scan is always
     * positioned at the first record of a group, which
     * means that this method moves to the
     * first underlying record.
     */
    @Override
    public void beforeFirst() {
        s.beforeFirst();
        moreGroups = s.next();
    }

    /**
     * Move to the next group.
     * The key of the group is determined by the
     * group values at the current record.
     * The method repeatedly reads underlying records until
     * it encounters a record having a different key.
     * The aggregation functions are called for each record
     * in the group.
     * The values of the grouping fields for the group are saved.
     */
    @Override
    public boolean next() {
        if (!moreGroups) {
            return false;
        }
        for (AggregateFn fn : aggFns) {
            fn.processFirst(s);
        }
        groupVal = new GroupValue(s, groupFields);
        while (moreGroups = s.next()) {
            GroupValue gv = new GroupValue(s, groupFields);
            if (!groupVal.equals(gv)) {
                break;
            }
            for (AggregateFn fn : aggFns)
                fn.processNext(s);
        }

        return true;
    }

    @Override
    public void close() {
        s.close();
    }

    /**
     * Get the Constant value of the specified field.
     * If the field is a group field, then its value can
     * be obtained from the saved group value.
     * Otherwise, the value is obtained from the
     * appropriate aggregation function.
     */
    @Override
    public Constant getVal(String fieldName) {
        if (groupFields.contains(fieldName)) {
            return groupVal.getVal(fieldName);
        }
        for (AggregateFn fn : aggFns) {
            if (fn.fieldName().equals(fieldName)) {
                return fn.value();
            }
        }
        throw new RuntimeException("field " + fieldName + " not found.");
    }

    /**
     * Get the integer value of the specified field.
     * If the field is a group field, then its value can
     * be obtained from the saved group value.
     * Otherwise, the value is obtained from the
     * appropriate aggregation function.
     */
    @Override
    public int getInt(String fieldName) {
        return getVal(fieldName).asInt();
    }

    /**
     * Get the String value of the specified field.
     * If the field is a group field, then its value can
     * be obtained from the saved group value.
     * Otherwise, the value is obtained from the
     * appropriate aggregation function.
     */
    @Override
    public String getString(String fieldName) {
        return getVal(fieldName).asString();
    }

    /**
     * Returns true if the specified field is either a
     * grouping field or created by an aggregation function.
     */
    @Override
    public boolean hasField(String fieldName) {
        if (groupFields.contains(fieldName)) {
            return true;
        }
        for (AggregateFn fn : aggFns) {
            if (fn.fieldName().equals(fieldName)) {
                return true;
            }
        }

        return false;
    }
}

package simpledb.materizlize;

import simpledb.query.Constant;
import simpledb.query.Scan;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GroupValue {
    private Map<String, Constant> vals;

    /**
     * Create a new group value, given the specified scan
     * and list of fields.
     * The values in the current record of each field are
     * stored.
     */
    public GroupValue(Scan s, List<String > fields) {
        vals = new HashMap<>();
        for (String fieldName : fields) {
            vals.put(fieldName, s.getVal(fieldName));
        }
    }

    /**
     * Return the Constant value of the specified field in the group.
     */
    public Constant getVal(String fieldName) {
        return vals.get(fieldName);
    }

    /**
     * Two GroupValue objects are equal if they have the same values
     * for their grouping fields.
     */
    public boolean equals(Object obj) {
        GroupValue gv = (GroupValue) obj;
        for (String fieldName : vals.keySet()) {
            Constant v1 = vals.get(fieldName);
            Constant v2 = gv.getVal(fieldName);
            if (!v1.equals(v2))
                return false;
        }
        return true;
    }

    /**
     * The hashcode of a GroupValue object is the sum of the
     * hashcodes of its field values.
     */
    public int hashCode() {
        int hashVal = 0;
        for (Constant c : vals.values()) {
            hashVal += c.hashCode();
        }
        return hashVal;

    }
}

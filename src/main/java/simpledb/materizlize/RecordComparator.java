package simpledb.materizlize;

import simpledb.query.Constant;
import simpledb.query.Scan;

import java.util.Comparator;
import java.util.List;

public class RecordComparator implements Comparator<Scan> {
    private List<String> fields;

    public RecordComparator(List<String> fields) {
        this.fields = fields;
    }

    @Override
    public int compare(Scan s1, Scan s2) {
        for (String fieldName : fields) {
            Constant val1 = s1.getVal(fieldName);
            Constant val2 = s2.getVal(fieldName);
            int result = val1.compareTo(val2);
            if (result != 0)
                return result;
        }
        return 0;
    }
}

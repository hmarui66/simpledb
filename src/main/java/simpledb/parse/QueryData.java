package simpledb.parse;

import simpledb.query.Predicate;

import java.util.Collection;
import java.util.List;

public class QueryData {
    private List<String> fields;
    private Collection<String> tables;
    private Predicate pred;
    public QueryData(List<String> fields, Collection<String> tables, Predicate pred) {
        this.fields = fields;
        this.tables = tables;
        this.pred = pred;
    }

    public List<String> fields() {
        return fields;
    }

    public Collection<String> tables() {
        return tables;
    }

    public Predicate pred() {
        return pred;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("select ");
        for (String fieldName: fields)
            result.append(fieldName).append(", ");
        result = new StringBuilder(result.substring(0, result.length() - 2)); // remove final comma
        result.append(" from ");
        for (String tblName : tables)
            result.append(tblName).append(", ");
        result = new StringBuilder(result.substring(0, result.length() - 2)); // remove final comma
        String predString = pred.toString();
        if (!predString.equals(""))
            result.append(" where ").append(predString);
        return result.toString();
    }
}

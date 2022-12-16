package simpledb.query;

import simpledb.plan.Plan;
import simpledb.record.Schema;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A predicate is a Boolean combination of terms.
 */
public class Predicate {
    private List<Term> terms = new ArrayList<>();

    public Predicate() {
    }

    public Predicate(Term t) {
        terms.add(t);
    }

    public void conjoinWith(Predicate pred) {
        terms.addAll(pred.terms);
    }

    public boolean isSatisfied(Scan s) {
        for (Term t : terms)
            if (!t.isSatisfied(s))
                return false;
        return true;
    }

    /**
     * Calculate the extent to which selection on the predicate
     * reduces the number of records output by a query.
     * FOr example if the reduction factor is 2, then the
     * predicate cuts the size of the output in half.
     */
    public int reductionFactor(Plan p) {
        int factor = 1;
        for (Term t : terms) {
            factor *= t.reductionFactor(p);
        }
        return factor;
    }

    /**
     * Return the sub-predicate that applies to the specified schema.
     */
    public Predicate selectSubPred(Schema sch) {
        Predicate result = new Predicate();
        for (Term t : terms)
            if (t.appliesTo(sch))
                result.terms.add(t);
        if (result.terms.size() == 0)
            return null;
        else
            return result;
    }

    /**
     * Return the sub-predicate consisting of terms that apply
     * to the union of the two specified schemas,
     * but not to either schema separately.
     */
    public Predicate joinSubPred(Schema sch1, Schema sch2) {
        Predicate result = new Predicate();
        Schema newSch = new Schema();
        newSch.addAll(sch1);
        newSch.addAll(sch2);
        for (Term t : terms)
            if (!t.appliesTo(sch1) && !t.appliesTo(sch2) && t.appliesTo(newSch))
                result.terms.add(t);
        if (result.terms.size() == 0)
            return null;
        else
            return result;
    }

    /**
     * Determine if there is a term of the form "F=c"
     * where F is the specified field and c is some constant.
     * If so, the method returns that constant.
     * If not, the method returns null.
     */
    public Constant equatesWithConstant(String fieldName) {
        for (Term t : terms) {
            Constant c = t.equatesWithConstant(fieldName);
            if (c != null)
                return c;
        }
        return null;
    }

    public String equatesWithField(String fieldName) {
        for (Term t : terms) {
            String s = t.equatesWithField(fieldName);
            if (s != null)
                return s;
        }
        return null;
    }

    public String toString() {
        Iterator<Term> iter = terms.iterator();
        if (!iter.hasNext())
            return "";
        StringBuilder result = new StringBuilder(iter.next().toString());
        while (iter.hasNext())
            result.append(" and ").append(iter.next().toString());
        return result.toString();
    }
}

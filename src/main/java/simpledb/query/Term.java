package simpledb.query;

import simpledb.plan.Plan;
import simpledb.record.Schema;

public class Term {
    private Expression lhs, rhs;

    public Term(Expression lhs, Expression rhs) {
        this.lhs = lhs;
        this.rhs = rhs;
    }

    public boolean isSatisfied(Scan s) {
        Constant lhsVal = lhs.evaluate(s);
        Constant rhsVal = rhs.evaluate(s);
        return rhsVal.equals(lhsVal);
    }

    public boolean appliesTo(Schema sch) {
        return lhs.appliesTo(sch) && rhs.appliesTo(sch);
    }

    public int reductionFactor(Plan p) {
        String lhsName, rhsName;
        if (lhs.isFieldName() && rhs.isFieldName()) {
            lhsName = lhs.asFieldName();
            rhsName = rhs.asFieldName();
            return Math.max(p.distinctValues(lhsName), p.distinctValues(rhsName));
        }
        if (lhs.isFieldName()) {
            lhsName = lhs.asFieldName();
            return p.distinctValues(lhsName);
        }
        if (rhs.isFieldName()) {
            rhsName = rhs.asFieldName();
            return p.distinctValues(rhsName);
        }
        // otherwise, the term equates constants
        if (lhs.asConstant().equals(rhs.asConstant()))
            return 1;
        else
            return Integer.MAX_VALUE;
    }

    public Constant equatesWithConstant(String fieldName) {
        if (lhs.isFieldName() && lhs.asFieldName().equals(fieldName) &&
                !rhs.isFieldName())
            return rhs.asConstant();
        else if (rhs.isFieldName() && rhs.asFieldName().equals(fieldName) &&
                !lhs.isFieldName())
            return lhs.asConstant();
        else
            return null;
    }

    public String equatesWithField(String fieldName) {
        if (lhs.isFieldName() && lhs.asFieldName().equals(fieldName) &&
                !rhs.isFieldName())
            return rhs.asFieldName();
        else if (rhs.isFieldName() && rhs.asFieldName().equals(fieldName) &&
                !lhs.isFieldName())
            return lhs.asFieldName();
        else
            return null;
    }

    public String toString() {
        return lhs.toString() + "=" + rhs.toString();
    }
}

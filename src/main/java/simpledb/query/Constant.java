package simpledb.query;

public class Constant implements Comparable<Constant> {
    private Integer iVal = null;
    private String sVal = null;

    public Constant(Integer iVal) {
        this.iVal = iVal;
    }

    public Constant(String sVal) {
        this.sVal = sVal;
    }

    public int asInt() {
        return iVal;
    }

    public String asString() {
        return sVal;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Constant c = (Constant) obj;
        return (iVal != null) ? iVal.equals(c.iVal) : sVal.equals(c.sVal);
    }

    @Override
    public int hashCode() {
        return (iVal != null) ? iVal.hashCode() : sVal.hashCode();
    }

    @Override
    public int compareTo(Constant c) {
        return (iVal != null) ? iVal.compareTo(c.iVal) : sVal.compareTo(c.sVal);
    }

    @Override
    public String toString() {
        return (iVal != null) ? iVal.toString() : sVal;
    }
}

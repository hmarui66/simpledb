package simpledb.record;

public class FieldInfo {
    public int type;
    public int length;

    public FieldInfo(int type, int length) {
        this.type = type;
        this.length = length;
    }
}

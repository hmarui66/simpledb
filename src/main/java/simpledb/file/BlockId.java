package simpledb.file;

public class BlockId {
    private final String filename;
    private final int blknum;

    public BlockId(String filename, int blknum) {
        this.filename = filename;
        this.blknum = blknum;
    }

    public String fileName() {
        return this.filename;
    }

    public int number() {
        return this.blknum;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BlockId blockId = (BlockId) o;
        return blknum == blockId.blknum && filename.equals(blockId.filename);
    }

    @Override
    public String toString() {
        return "[file " + filename + ", block " + blknum + ']';
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }
}

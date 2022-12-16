package simpledb.metadata;

public class StatInfo {
    private int numBlocks;
    private int numRecs;

    public StatInfo(int numBlocks, int numRecs) {
        this.numBlocks = numBlocks;
        this.numRecs = numRecs;
    }

    public int blocksAccessed() {
        return numBlocks;
    }

    public int recordsOutput() {
        return numRecs;
    }

    public int distinctValues(String fieldName) {
        return 1 + (numRecs / 3); // This is wildly inaccurate.
    }
}

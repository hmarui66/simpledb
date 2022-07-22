package tx.recovery;

import file.Page;
import log.LogMgr;

public class CheckpointRecord implements LogRecord {

    public CheckpointRecord() {
    }

    @Override
    public int op() {
        return CHECKPOINT;
    }

    @Override
    public int txNumber() {
        return -1; // dummy value
    }

    @Override
    public void undo(Transaction tx) {
    }

    public String toString() {
        return "<CHECKPOINT>";
    }

    public static int writeToLog(LogMgr lm) {
        byte[] rec = new byte[Integer.BYTES];
        Page p = new Page(rec);
        p.setInt(0, CHECKPOINT);
        return lm.append(rec);
    }
}

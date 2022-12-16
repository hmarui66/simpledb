package simpledb.tx.recovery;

import simpledb.file.Page;
import simpledb.tx.Transaction;

public interface LogRecord {
    static final int CHECKPOINT = 0,
            START = 1,
            COMMIT = 2,
            ROLLBACK = 3,
            SETINT = 4,
            SETSTRING = 5;

    int op();
    int txNumber();
    void undo (Transaction tx);

    static LogRecord createLogRecord(byte[] bytes) {
        Page p = new Page(bytes);
        return switch(p.getInt(0)) {
            case CHECKPOINT -> new CheckpointRecord();
            case START -> new StartRecord(p);
            case COMMIT -> new CommitRecord(p);
            case ROLLBACK -> new RollbackRecord(p);
            case SETINT -> new SetIntRecord(p);
            case SETSTRING -> new SetStringRecord(p);
            default -> throw new IllegalStateException("Unexpected value: " + p.getInt(0));
        };
    }
}

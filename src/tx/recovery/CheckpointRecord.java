package tx.recovery;

public class CheckpointRecord implements LogRecord {
    @Override
    public int op() {
        return 0;
    }

    @Override
    public int txNumber() {
        return 0;
    }

    @Override
    public void undo(Transaction tx) {

    }
}

package tx.recovery;

import file.Page;

public class RollbackRecord implements LogRecord {
    public RollbackRecord(Page p) {
    }

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

package tx.recovery;

import file.Page;

public class StartRecord implements LogRecord {
    public StartRecord(Page p) {
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

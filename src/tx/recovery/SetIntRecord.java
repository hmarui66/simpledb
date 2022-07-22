package tx.recovery;

import file.Page;

public class SetIntRecord implements LogRecord {
    public SetIntRecord(Page p) {
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

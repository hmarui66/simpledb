package simpledb.tx.recovery;

import simpledb.file.BlockId;
import simpledb.file.Page;
import simpledb.log.LogMgr;
import simpledb.tx.Transaction;

public class SetStringRecord implements LogRecord {
    private int txnum, offset;
    private String val;
    private BlockId blk;

    public SetStringRecord(Page p) {
        int tpos = Integer.BYTES;
        txnum = p.getInt(tpos);
        int fpos = tpos + Integer.BYTES;
        String filename = p.getString(fpos);
        int bpos = fpos + Page.maxLength(filename.length());
        int blknum = p.getInt(bpos);
        blk = new BlockId(filename, blknum);
        int opos = bpos + Integer.BYTES;
        offset = p.getInt(opos);
        int vpos = opos + Integer.BYTES;
        val = p.getString(vpos);
    }

    @Override
    public int op() {
        return SETSTRING;
    }

    @Override
    public int txNumber() {
        return txnum;
    }

    public String toString() {
        return "<SETSTRING " + txnum + " " + blk + " " + offset + " " + val + ">";
    }

    @Override
    public void undo(Transaction tx) {
        tx.pin(blk);
        tx.setString(blk, offset, val, false); // don't log the undo!
        tx.unpin(blk);
    }

    public static int writeToLog(LogMgr lm, int txnum, BlockId blk, int offset, String val) {
        int tpos = Integer.BYTES;
        int fpos = tpos + Integer.BYTES;
        int bpos = fpos + Page.maxLength(blk.fileName().length());
        int opos = bpos + Integer.BYTES;
        int vpos = opos + Integer.BYTES;
        int reclen = vpos + Page.maxLength(val.length());
        byte[] rec = new byte[reclen];
        Page p = new Page(rec);
        p.setInt(0, SETSTRING);
        p.setInt(tpos, txnum);
        p.setString(fpos, blk.fileName());
        p.setInt(bpos, blk.number());
        p.setInt(opos, offset);
        p.setString(vpos, val);
        return lm.append(rec);

    }
}

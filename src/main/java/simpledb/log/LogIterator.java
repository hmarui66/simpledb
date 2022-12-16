package simpledb.log;

import simpledb.file.BlockId;
import simpledb.file.FileMgr;
import simpledb.file.Page;

import java.util.Iterator;

public class LogIterator implements Iterator<byte[]> {
    private FileMgr fm;
    private BlockId blk;
    private Page p;
    private int currentpos;
    private int boundary;

    public LogIterator(FileMgr fm, BlockId blk) {
        this.fm = fm;
        this.blk = blk;
        byte[] b = new byte[fm.blocksize()];
        p = new Page(b);
        moveToBlock(blk);
    }

    @Override
    public boolean hasNext() {
        return currentpos < fm.blocksize() || blk.number() > 0;
    }

    @Override
    public byte[] next() {
        if (currentpos == fm.blocksize()) {
            blk = new BlockId(blk.fileName(), blk.number() -1);
            moveToBlock(blk);
        }
        byte[] rec = p.getBytes(currentpos);
        currentpos += Integer.BYTES + rec.length;
        return rec;
    }

    private void moveToBlock(BlockId blk) {
        fm.read(blk, p);
        boundary = p.getInt(0);
        currentpos = boundary;
    }
}

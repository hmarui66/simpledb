package simpledb.tx;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferMgr;
import simpledb.file.BlockId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BufferList {
    private Map<BlockId, Buffer> buffers = new HashMap<>();
    private List<BlockId> pins = new ArrayList<>();
    private BufferMgr bm;
    public BufferList(BufferMgr bm) {
        this.bm = bm;
    }

    public Buffer getBuffer(BlockId blk) {
        return buffers.get(blk);
    }

    public void pin(BlockId blk) {
        Buffer buff = bm.pin(blk);
        buffers.put(blk, buff);
        pins.add(blk);
    }

    public void unpin(BlockId blk) {
        Buffer buff = buffers.get(blk);
        bm.unpin(buff);
        pins.remove(blk);
        if (!pins.contains(blk)) {
            buffers.remove(blk);
        }
    }

    public void unpinAll() {
        for (BlockId blk : pins) {
            Buffer buff = buffers.get(blk);
            bm.unpin(buff);
        }
        buffers.clear();
        pins.clear();
    }


}

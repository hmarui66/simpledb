package simpledb.buffer;

import simpledb.file.BlockId;
import simpledb.file.FileMgr;
import simpledb.file.Page;
import simpledb.log.LogMgr;

import java.io.File;

public class BufferTest {
    public static void main(String[] args) {
        var fm = new FileMgr(new File("dbdir"), 400);
        var lm = new LogMgr(fm, "testfile");
        BufferMgr bm = new BufferMgr(fm, lm, 3);
        Buffer buff1 = bm.pin(new BlockId("testfile", 1));
        Page p = buff1.contents();
        int n = p.getInt(80);
        p.setInt(80, n + 1); // この変更は後続の pin 処理で Buffer の再割当て時に disk に書き込まれる
        // pin した buffer を更新したことをセット？
        buff1.setModified(1, 0);
        System.out.println("The new value is " + (n + 1));
        bm.unpin(buff1);
        Buffer buff2 = bm.pin(new BlockId("testfile", 2));
        Buffer buff3 = bm.pin(new BlockId("testfile", 3));
        Buffer buff4 = bm.pin(new BlockId("testfile", 4));

        bm.unpin(buff2);
        buff2 = bm.pin(new BlockId("testfile", 1));
        Page p2 = buff2.contents();
        p2.setInt(80, 9999); // この変更は flush の機会がないので disk に書き込まれない
        buff2.setModified(1, 0);
        bm.unpin(buff2);
    }
}

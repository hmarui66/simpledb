package simpledb.file;

import java.io.File;

public class FileTest {
    public static void main(String[] args) {
        System.out.println();
        var dir = new File("dbdir");
        var fm = new FileMgr(dir, 400);
        var blk = new BlockId("testfile", 2);
        var p1 = new Page(fm.blocksize());
        int pos1 = 88;
        p1.setString(pos1, "abcdefghijklm");
        var size = Page.maxLength("abcdefghijklm".length());
        var pos2 = pos1 + size;
        p1.setInt(pos2, 345);
        fm.write(blk, p1);

        var p2 = new Page(fm.blocksize());
        fm.read(blk, p2);
        System.out.println("offset " + pos2 + " contains " + p2.getInt(pos2));
        System.out.println("offset " + pos1 + " contains " + p2.getString(pos1));
    }
}

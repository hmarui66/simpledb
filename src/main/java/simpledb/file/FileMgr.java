package simpledb.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

public class FileMgr {
    private final File dbDirectory;


    private final int blocksize;
    private final boolean isNew;
    private final Map<String, RandomAccessFile> openFiles = new HashMap<>();

    public FileMgr(File dbDirectory, int blocksize) {
        this.dbDirectory = dbDirectory;
        this.blocksize = blocksize;
        isNew = !dbDirectory.exists();

        if (isNew)
            dbDirectory.mkdirs();

        // remove any leftover temporary tables
        for (String filename : dbDirectory.list())
            if (filename.startsWith("temp"))
                new File(dbDirectory, filename).delete();
    }

    public boolean isNew() {
        return isNew;
    }

    public int blocksize() {
        return blocksize;
    }

    public synchronized void read(BlockId blk, Page p) {
        try {
            RandomAccessFile f = getFile(blk.fileName());
            f.seek(blk.number() * blocksize);
            f.getChannel().read(p.contents());
        } catch (IOException e) {
            throw new RuntimeException("cannot read block " + blk, e);
        }
    }

    public synchronized void write(BlockId blk, Page p) {
        try {
            RandomAccessFile f = getFile(blk.fileName());
            f.seek(blk.number() * blocksize);
            f.getChannel().write(p.contents());
        } catch (IOException e) {
            throw new RuntimeException("cannot write block " + blk, e);
        }
    }

    public synchronized BlockId append(String fileName) {
        int newblknum = length(fileName);
        BlockId blk = new BlockId(fileName, newblknum);
        byte[] b = new byte[blocksize];
        try {
            RandomAccessFile f = getFile(blk.fileName());
            f.seek(blk.number() * blocksize);
            f.write(b);
        } catch (IOException e) {
            throw new RuntimeException("cannot append block " + blk, e);
        }
        return blk;
    }

    public int length(String fileName) {
        try {
            RandomAccessFile f = getFile(fileName);
            return (int) (f.length() / blocksize);
        } catch (IOException e) {
            throw new RuntimeException("cannot access " + fileName, e);
        }
    }

    private RandomAccessFile getFile(String fileName) throws IOException {
        RandomAccessFile f = openFiles.get(fileName);
        if (f == null) {
            File dbTable = new File(dbDirectory, fileName);
            f = new RandomAccessFile(dbTable, "rws");
            openFiles.put(fileName, f);
        }
        return f;
    }


}

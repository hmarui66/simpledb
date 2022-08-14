package server;

import buffer.BufferMgr;
import file.FileMgr;
import log.LogMgr;

import java.io.File;

public class SimpleDB {
    public static String LOG_FILE = "simpledb.log";

    private FileMgr fm;
    private BufferMgr bm;
    private LogMgr lm;

    public SimpleDB(String dirname, int blocksize, int buffsize) {
        File dbDirectory = new File(dirname);
        fm = new FileMgr(dbDirectory, blocksize);
        lm = new LogMgr(fm, LOG_FILE);
        bm = new BufferMgr(fm, lm, buffsize);
    }

    public FileMgr fileMgr() {
        return fm;
    }

    public LogMgr logMgr() {
        return lm;
    }

    public BufferMgr bufferMgr() {
        return bm;
    }
}

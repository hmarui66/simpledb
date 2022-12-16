package simpledb.log;

import simpledb.file.BlockId;
import simpledb.file.FileMgr;
import simpledb.file.Page;

import java.util.Iterator;

public class LogMgr {
    private final FileMgr fm;
    private final String logfile;
    private Page logpage;
    private BlockId currentblk;
    private int lastSavedLSN = 0;
    private int latestLSN = 0;

    public LogMgr(FileMgr fm, String logfile) {
        this.fm = fm;
        // ログファイルの名称は単一
        this.logfile = logfile;
        // FileMgr のブロックサイズに準拠
        byte[] b = new byte[fm.blocksize()];
        logpage = new Page(b);
        int logsize = fm.length(logfile);
        // すでにログが記録されているかどうか
        if (logsize == 0) {
            // 記録されていなければ新しい Block を追加
            currentblk = appendNewBlock();
        } else {
            // 記録されていれば、最後のブロックを読み取り
            currentblk = new BlockId(logfile, logsize - 1);
            fm.read(currentblk, logpage);
        }
    }

    public void flush(int lsn) {
        // flush 対象の LSN が最後に保存された LSN 以上である必要あり
        if (lsn >= lastSavedLSN) {
            flush();
        }
    }

    public Iterator<byte[]> iterator() {
        flush();
        return new LogIterator(fm, currentblk);
    }

    public synchronized int append(byte[] logrec) {
        // Page の先頭に記録されている境界値を取得
        int boundary = logpage.getInt(0);
        // ログレコードのサイズ取得
        int recsize = logrec.length;
        // 書き込み対象のサイズ計算(ログレコード + Int のバイト数)
        int bytesneeded = recsize + Integer.BYTES;
        // Page の残りサイズが書き込み対象よりも多いかどうかの判定
        if (boundary - bytesneeded < Integer.BYTES) {
            // 足りない場合は一旦 flush してサイズを確保
            flush();
            currentblk = appendNewBlock();
            boundary = logpage.getInt(0);
        }
        int recpos = boundary - bytesneeded;
        logpage.setBytes(recpos, logrec);
        logpage.setInt(0, recpos);
        latestLSN += 1;
        return latestLSN;
    }

    private BlockId appendNewBlock() {
        // 新しい BLock を追加
        BlockId blk = fm.append(logfile);
        // Page の先頭に書き込み可能なサイズ(初期値は Block サイズ)を記録
        logpage.setInt(0, fm.blocksize());
        // ファイルにも書き出し <- logpage ってゴミデータ残っちゃったりしてない...?
        fm.write(blk, logpage);
        return blk;
    }

    private void flush() {
        fm.write(currentblk, logpage);
        lastSavedLSN = latestLSN;
    }

}

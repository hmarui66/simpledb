package simpledb.tx;

import simpledb.file.BlockId;

public interface Transaction {
     void commit();
     void rollback();
     void recover();
     void pin(BlockId blk);
     void unpin(BlockId blk);
     int getInt(BlockId blk, int offset);
     String getString(BlockId blk, int offset);
     void setInt(BlockId blk, int offset, int val, boolean okToLog);
     void setString(BlockId blk, int offset, String val, boolean okToLog);
     int size(String filename);
     BlockId append(String filename);
     int blockSize();
     int availableBuffs();
}

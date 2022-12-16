package simpledb;

import java.io.IOException;
import java.io.RandomAccessFile;

public class SimpleDb {
    public static void main(String[] args) throws IOException {
        RandomAccessFile f = new RandomAccessFile("junk", "rws");
        f.seek(0);
        int n = f.readInt();
        f.seek(0);
        f.writeInt(n+1);
        f.close();

        System.out.println("Hello!");
    }
}

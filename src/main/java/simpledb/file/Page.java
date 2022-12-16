package simpledb.file;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class Page {
    private final ByteBuffer bb;
    public static final Charset CHARSET = StandardCharsets.UTF_8;

    public Page(int blocksize) {
        bb = ByteBuffer.allocateDirect(blocksize);
    }

    // A constructor for creating data buffers
    public Page(byte[] b) {
        bb = ByteBuffer.wrap(b);
    }

    public int getInt(int offset) {
        return bb.getInt(offset);
    }

    public void setInt(int offset, int n) {
        bb.putInt(offset, n);
    }

    public Short getShort(int offset) {
        return bb.getShort(offset);
    }
    public void setShortInt(int offset, Short n) {
        bb.putShort(n);
    }
    public boolean getBoolean(int offset) {
        return bb.get(offset) == 1;
    }
    public void setBoolean(int offset, boolean b) {
        bb.put(b ? (byte)0b1 : (byte) 0b0);
    }

    public byte[] getBytes(int offset) {
        bb.position(offset);
        int length = bb.getInt();
        byte[] b = new byte[length];
        bb.get(b);
        return b;
    }

    public void setBytes(int offset, byte[] b) {
        bb.position(offset);
        bb.putInt(b.length);
        bb.put(b);
    }

    public String getString(int offset) {
        byte[] b = getBytes(offset);
        return new String(b, CHARSET);
    }

    public void setString(int offset, String s) {
        byte[] b = s.getBytes(CHARSET);
        setBytes(offset, b);
    }


    public static int maxLength(int strlen) {
        float bytesPerChar = CHARSET.newEncoder().maxBytesPerChar();
        return Integer.BYTES + (strlen * (int) bytesPerChar);
    }

    public ByteBuffer contents() {
        bb.position(0);
        return bb;
    }
}

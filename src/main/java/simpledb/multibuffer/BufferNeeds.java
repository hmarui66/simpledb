package simpledb.multibuffer;

public class BufferNeeds {
    /**
     * This method considers the various roots
     * of the specified output size (in blocks),
     * and returns the highest root that is less than
     * the number of available buffers
     */
    public static int bestRoot(int available, int size) {
        int avail = available - 2; // 2 つを予約
        if (avail <= 1) {
            return 1;
        }
        int k = Integer.MAX_VALUE;
        double i = 1.0;
        while (k > avail) {
            i++;
            k = (int)Math.ceil(Math.pow(size, 1/i));
        }
        return k;
    }

    /**
     * This method considers the various factors
     * of the specified output size (in blocks),
     * and returns the highest factor that is less than
     * the number of available buffers.
     */
    public static int bestFactor(int available, int size) {
        int avail = available - 2; // 2 つを予約
        if (avail <= 1) {
            return 1;
        }
        int k = size;
        double i = 1.0;
        while (k > avail) {
            i++;
            k = (int)Math.ceil(size / i);
        }
        return k;
    }
}

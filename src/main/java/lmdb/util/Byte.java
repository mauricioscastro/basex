package lmdb.util;

public class Byte {

    public static void setLong(long value, byte[] array, int offset) {
        array[offset]   = (byte)(0xff & (value >> 56));
        array[offset+1] = (byte)(0xff & (value >> 48));
        array[offset+2] = (byte)(0xff & (value >> 40));
        array[offset+3] = (byte)(0xff & (value >> 32));
        array[offset+4] = (byte)(0xff & (value >> 24));
        array[offset+5] = (byte)(0xff & (value >> 16));
        array[offset+6] = (byte)(0xff & (value >> 8));
        array[offset+7] = (byte)(0xff & value);
    }

    public static void setLong(long value, byte[] array) {
        setLong(value,array,0);
    }

    public static long getLong(byte[] array, int offset) {
        return ((long)(array[offset]   & 0xff) << 56) |
                ((long)(array[offset+1] & 0xff) << 48) |
                ((long)(array[offset+2] & 0xff) << 40) |
                ((long)(array[offset+3] & 0xff) << 32) |
                ((long)(array[offset+4] & 0xff) << 24) |
                ((long)(array[offset+5] & 0xff) << 16) |
                ((long)(array[offset+6] & 0xff) << 8) |
                ((long)(array[offset+7] & 0xff));
    }

    public static long getLong(byte[] array) {
        return getLong(array,0);
    }


    public static void setInt(int value, byte[] array, int offset) {
        array[offset]   = (byte)(0xff & (value >> 24));
        array[offset+1] = (byte)(0xff & (value >> 16));
        array[offset+2] = (byte)(0xff & (value >> 8));
        array[offset+3] = (byte)(0xff & value);
    }

    public static void setInt(int value, byte[] array) { setInt(value, array, 0); }


    public static int getInt(byte[] array, int offset) {
         return ((int)(array[offset] & 0xff) << 24) |
                ((int)(array[offset+1] & 0xff) << 16) |
                ((int)(array[offset+2] & 0xff) << 8) |
                ((int)(array[offset+3] & 0xff));
    }

    public static int getInt(byte[] array) { return getInt(array, 0); }

    public static byte[] getBytes(int value) {
        return new byte[] {
            (byte) (value >> 24),
            (byte) (value >> 16),
            (byte) (value >> 8),
            (byte) value};
    }

    public static byte[] lmdbkey(byte[] docid, int pre) {
        return new byte[] {
                docid[0],
                docid[1],
                docid[2],
                docid[3],
                (byte)(pre >> 24),
                (byte)(pre >> 16),
                (byte)(pre >> 8),
                (byte)pre
        };
    }
}

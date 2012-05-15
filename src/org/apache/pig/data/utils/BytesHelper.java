package org.apache.pig.data.utils;

public class BytesHelper {
    private static final int[] mask = {0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80};
    private static final int[] invMask = {0xFE, 0xFD, 0xFB, 0xF7, 0xEF, 0xDF, 0xBF, 0x7F};

    public static boolean getBitByPos(byte byt, int pos) {
        return (byt & mask[pos]) > 0;
    }

    public static byte setBitByPos(byte byt, boolean bool, int pos) {
        if (bool) {
            return (byte)((int)byt | mask[pos]);
        } else {
            return (byte)((int)byt & invMask[pos]);
        }
    }
}

package lmdb;

import java.nio.charset.StandardCharsets;

public class Constants extends org.fusesource.lmdbjni.Constants {
    public static String string(byte value[], int off, int len) {
        if (value == null) {
            return null;
        }
        return new String(value, off, len, StandardCharsets.UTF_8);
    }
}

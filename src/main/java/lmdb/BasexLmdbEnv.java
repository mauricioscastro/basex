package lmdb;


import org.fusesource.lmdbjni.Env;
import org.fusesource.lmdbjni.LMDBException;

public class BasexLmdbEnv {

    public static Env getEnv(String path, long size, int flags) {
        Env env = create(size);
        int c = 0;
        while(++c < 10) {
            try {
                env.open(path, flags);
                break;
            } catch (LMDBException le) {
                if(le.getErrorCode() == 16) try {
                    env = create(size);
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
        return env;
    }

    private static Env create(long size) {
        Env env = new Env();
        env.setMapSize(size);
        env.setMaxDbs(16);
        return env;
    }
}

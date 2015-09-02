package lmdb.basex;

import org.fusesource.lmdbjni.Env;

public class LmdbDataManager {

    private static Env env;

    public LmdbDataManager(String home) {

        env = new Env(home);
        env.setMaxDbs(Long.MAX_VALUE/10);
    }
}

package lmdb.basex;

import org.basex.core.StaticOptions;
import org.basex.io.IOFile;

class LmdbStaticOptions extends StaticOptions {

    public LmdbStaticOptions() {
        super(false);
        set(DBPATH, System.getProperty("java.io.tmpdir", "/tmp"));
    }

    @Override
    public IOFile dbpath(final String name) {
        return new IOFile(get(DBPATH));
    }
}

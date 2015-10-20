package lmdb.basex;

import org.apache.commons.io.IOUtils;
import org.basex.core.MainOptions;
import org.basex.data.Data;
import org.basex.index.ft.FTBuilder;
import org.basex.index.ft.FTIndex;
import org.basex.io.IO;
import org.fusesource.lmdbjni.Database;
import org.fusesource.lmdbjni.Transaction;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import static lmdb.basex.LmdbDataManager.env;
import static lmdb.basex.LmdbDataManager.ftindexxdb;
import static lmdb.basex.LmdbDataManager.ftindexydb;
import static lmdb.basex.LmdbDataManager.ftindexzdb;
import static lmdb.util.Byte.lmdbkey;
import static org.basex.data.DataText.DATAFTX;

public class LmdbFTBuilder extends FTBuilder {

    private byte[] docid;

    public LmdbFTBuilder(final byte[] docid, final Data data, final MainOptions options) throws IOException {
        super(data, options);
        this.docid = docid;
    }

    @Override
    public FTIndex build() throws IOException {
        _build();
        copyIndex(DATAFTX + 'x', ftindexxdb);
        copyIndex(DATAFTX + 'y', ftindexydb);
        copyIndex(DATAFTX + 'z', ftindexzdb);
        data.meta.dbfile("swl").file().delete();
        return null;
    }

    private void copyIndex(final String name, Database db) throws IOException {

        Transaction tx = env.createWriteTransaction();

        File f = data.meta.dbfile(name).file();

        byte[] b = new byte[IO.BLOCKSIZE];
        BufferedInputStream idx = new BufferedInputStream(new FileInputStream(f), 1024*16);

        int actual = -1;
        int ref = 0;
        int txcount = 0;

        while((actual = IOUtils.read(idx, b)) > 0) {
            if(actual < b.length) {
                byte[] nb = new byte[actual];
                System.arraycopy(b,0,nb,0,actual);
                b = nb;
            }
            db.put(tx, lmdbkey(docid,ref++), b);
            if(++txcount > 10000) {
                tx.commit();
                tx = env.createWriteTransaction();
                txcount = 0;
            }
        }
        if(txcount > 0) tx.commit();
        else tx.close();

        f.delete();
    }

}

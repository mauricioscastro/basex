package lmdb.basex;

import org.apache.commons.io.IOUtils;
import org.basex.core.MainOptions;
import org.basex.data.Data;
import org.basex.index.value.DiskValues;
import org.basex.index.value.DiskValuesBuilder;
import org.basex.io.IO;
import org.fusesource.lmdbjni.Database;
import org.fusesource.lmdbjni.Transaction;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import static lmdb.basex.LmdbDataManager.attindexldb;
import static lmdb.basex.LmdbDataManager.attindexrdb;
import static lmdb.basex.LmdbDataManager.env;
import static lmdb.basex.LmdbDataManager.txtindexldb;
import static lmdb.basex.LmdbDataManager.txtindexrdb;
import static lmdb.util.Byte.lmdbkey;
import static org.basex.data.DataText.DATAATV;
import static org.basex.data.DataText.DATATXT;


public class LmdbValuesBuilder extends DiskValuesBuilder {

    private byte[] docid;

    public LmdbValuesBuilder(final byte[] docid, final Data data, final MainOptions options, final boolean text) {
        super(data, options, text);
        this.docid = docid;
    }

    @Override
    public DiskValues build() throws IOException {
        _build();
        final String f = text ? DATATXT : DATAATV;
        copyIndex(data.meta.dbfile(f + 'l').file(), text ? txtindexldb : attindexldb, docid);
        copyIndex(data.meta.dbfile(f + 'r').file(), text ? txtindexrdb : attindexrdb, docid);
        return null;
    }

    static void copyIndex(final File file, Database db, byte[] did) throws IOException {

        Transaction tx = env.createWriteTransaction();

        byte[] b = new byte[IO.BLOCKSIZE];
        BufferedInputStream idx = new BufferedInputStream(new FileInputStream(file), 1024*16);

        int actual = -1;
        int ref = 0;
        int txcount = 0;
        int tot = 0;

        while((actual = IOUtils.read(idx, b)) > 0) {
            tot += actual;
            if(actual < b.length) {
                byte[] nb = new byte[actual];
                System.arraycopy(b,0,nb,0,actual);
                b = nb;
            }
            db.put(tx, lmdbkey(did,ref++), b);
            if(++txcount > 10000) {
                tx.commit();
                tx = env.createWriteTransaction();
                txcount = 0;
            }
        }
        db.put(tx, LmdbDataAccess.getLenKey(did), lmdb.util.Byte.getBytes(tot));
        tx.commit();

        file.delete();
    }
}

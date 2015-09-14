package lmdb.basex;


import org.basex.data.MetaData;
import org.basex.io.IO;
import org.fusesource.lmdbjni.Database;
import org.fusesource.lmdbjni.Env;
import org.fusesource.lmdbjni.Transaction;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import static lmdb.util.Byte.getInt;
import static lmdb.util.Byte.lmdbkey;

public class TableLmdbAccessBuilder extends TableLmdbAccess {

    private Env env;
    private RandomAccessFile tempBuffer = null;
    private File tmpFile;

    public TableLmdbAccessBuilder(final MetaData md, Env env, Database db, byte[] docid) throws IOException {
        super(md,null,db,docid);
        this.env = env;
        tmpFile = File.createTempFile("tbl.",".tmp",null);
        tmpFile.deleteOnExit();
        tempBuffer = new RandomAccessFile(tmpFile,"rw");
    }

    @Override
    public void close() throws IOException {

        try {
            int c = 0;

            tempBuffer.getFD().sync();
            tempBuffer.seek(0);

            tx = env.createWriteTransaction();

            for (int k = 0; k < (tempBuffer.length() / IO.NODESIZE); k++) {
                byte v[] = new byte[IO.NODESIZE];
                tempBuffer.readFully(v);

                db.put(tx, lmdbkey(docid, k), v);
                c++;
                if (c > 1024 * 10) {
                    this.tx.commit();
                    c = 0;
                    tx = env.createWriteTransaction();
                }
            }
            if (c > 0) tx.commit();
            else tx.close();

            tempBuffer.close();

        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
        tmpFile.delete();
    }

    @Override
    protected byte[] get(int pre) {
        byte[] v = new byte[IO.NODESIZE];
        try {
            tempBuffer.seek(pre*IO.NODESIZE);
            tempBuffer.readFully(v);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return v;
    }

    @Override
    protected byte[] get(byte[] key) {
        return get(getInt(key,4));
    }

    @Override
    protected void put(byte[] key, byte[] value) {
        try {
            tempBuffer.seek(getInt(key,4)*IO.NODESIZE);
            tempBuffer.write(value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

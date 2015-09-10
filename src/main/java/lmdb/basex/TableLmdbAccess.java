package lmdb.basex;

import org.basex.data.MetaData;
import org.basex.io.IO;
import org.basex.io.random.TableAccess;
import org.fusesource.lmdbjni.Database;
import org.fusesource.lmdbjni.Transaction;

import java.io.IOException;

public class TableLmdbAccess extends TableAccess {

    private Transaction tx;
    private Database db;
    private byte[] docid;

    public TableLmdbAccess(final MetaData md, final Transaction tx, Database db, byte[] docid) {
        super(md);
        this.tx = tx;
        this.db = db;
        this.docid = docid;
    }

    @Override
    public void flush(boolean all) throws IOException {

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public boolean lock(boolean write) {
        return true;
    }

    @Override
    public int read1(int p, int o) {
        int r = -1;
        byte[] x = db.get(tx,key(p));
        r = x[o] & 0xFF;
        return r;
    }

    @Override
    public int read2(int p, int o) {
        byte[] b = db.get(tx,key(p));
        return (int)(((b[o] & 0xFF) << 8) + (b[o + 1] & 0xFF));
    }

    @Override
    public int read4(int p, int o) {
        byte[] b = db.get(tx, key(p));
        return (int)(((b[o] & 0xFF) << 24) + ((b[o + 1] & 0xFF) << 16) + ((b[o + 2] & 0xFF) << 8) + (b[o + 3] & 0xFF));
    }

    @Override
    public long read5(int p, int o) {
        byte[] b = db.get(tx, key(p));
        return (long) (((long) (b[o] & 0xFF) << 32) + ((long) (b[o + 1] & 0xFF) << 24) + ((b[o + 2] & 0xFF) << 16) + ((b[o + 3] & 0xFF) << 8) + (b[o + 4] & 0xFF));
    }

    @Override
    public void write1(int p, int o, int v) {
        dirty();
        byte[] k = key(p);
        final byte[] b = db.get(tx, k);
        b[o] = (byte) v;
        db.put(tx, k, b);
    }

    @Override
    public void write2(int p, int o, int v) {
        dirty();
        byte[] k = key(p);
        final byte[] b = db.get(tx, k);
        b[o] = (byte) (v >>> 8);
        b[o + 1] = (byte) v;
        db.put(tx, k, b);
    }

    @Override
    public void write4(int p, int o, int v) {
        dirty();
        byte[] k = key(p);
        final byte[] b = db.get(tx, k);
        b[o]     = (byte) (v >>> 24);
        b[o + 1] = (byte) (v >>> 16);
        b[o + 2] = (byte) (v >>> 8);
        b[o + 3] = (byte) v;
        db.put(tx, k, b);
    }

    @Override
    public void write5(int p, int o, long v) {
        dirty();
        byte[] k = key(p);
        final byte[] b = db.get(tx, k);
        b[o]     = (byte) (v >>> 32);
        b[o + 1] = (byte) (v >>> 24);
        b[o + 2] = (byte) (v >>> 16);
        b[o + 3] = (byte) (v >>> 8);
        b[o + 4] = (byte) v;
        db.put(tx, k, b);
    }

    @Override
    protected void dirty() {
        dirty = true;
    }

    @Override
    protected void copy(byte[] entries, int pre, int last) {
        for(int i = pre; i < last; i++) db.put(tx, key(i), entries);
        dirty();
    }

    @Override
    public void delete(int pre, int nr) {
        if(nr == 0) return;
        for(int i = pre; i < pre+nr; i++) db.delete(tx, key(i));
        dirty();
    }

    @Override
    public void insert(int pre, byte[] entries) {
        if (entries.length == 0) return;
        meta.size += (pre + (entries.length >>> IO.NODEPOWER)) - pre;
        db.put(tx, key(pre), entries);
        dirty();
    }

    private byte[] key(int pre) {
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

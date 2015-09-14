package lmdb.basex;

import org.basex.data.MetaData;
import org.basex.io.IO;
import org.basex.io.random.TableAccess;
import org.fusesource.lmdbjni.Database;
import org.fusesource.lmdbjni.Transaction;

import java.io.IOException;

import static lmdb.util.Byte.lmdbkey;

public class TableLmdbAccess extends TableAccess {

    protected Transaction tx;
    protected Database db;
    protected byte[] docid;

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
        byte[] x = get(p);
        r = x[o] & 0xFF;
        return r;
    }

    @Override
    public int read2(int p, int o) {
        byte[] b = get(p);
        return (int)(((b[o] & 0xFF) << 8) + (b[o + 1] & 0xFF));
    }

    @Override
    public int read4(int p, int o) {
        byte[] b = get(p);
        return (int)(((b[o] & 0xFF) << 24) + ((b[o + 1] & 0xFF) << 16) + ((b[o + 2] & 0xFF) << 8) + (b[o + 3] & 0xFF));
    }

    @Override
    public long read5(int p, int o) {
        byte[] b = get(p);
        return (long) (((long) (b[o] & 0xFF) << 32) + ((long) (b[o + 1] & 0xFF) << 24) + ((b[o + 2] & 0xFF) << 16) + ((b[o + 3] & 0xFF) << 8) + (b[o + 4] & 0xFF));
    }

    @Override
    public void write1(int p, int o, int v) {
        dirty();
        byte[] k = lmdbkey(docid, p);
        final byte[] b = get(k);
        b[o] = (byte) v;
        put(k, b);
    }

    @Override
    public void write2(int p, int o, int v) {
        dirty();
        byte[] k = lmdbkey(docid, p);
        final byte[] b = get(k);
        b[o] = (byte) (v >>> 8);
        b[o + 1] = (byte) v;
        put(k, b);
    }

    @Override
    public void write4(int p, int o, int v) {
        dirty();
        byte[] k = lmdbkey(docid, p);
        final byte[] b = get(k);
        b[o]     = (byte) (v >>> 24);
        b[o + 1] = (byte) (v >>> 16);
        b[o + 2] = (byte) (v >>> 8);
        b[o + 3] = (byte) v;
        put(k, b);
    }

    @Override
    public void write5(int p, int o, long v) {
        dirty();
        byte[] k = lmdbkey(docid, p);
        final byte[] b = get(k);
        b[o]     = (byte) (v >>> 32);
        b[o + 1] = (byte) (v >>> 24);
        b[o + 2] = (byte) (v >>> 16);
        b[o + 3] = (byte) (v >>> 8);
        b[o + 4] = (byte) v;
        put(k, b);
    }

    @Override
    protected void dirty() {
        dirty = true;
    }

    @Override
    protected void copy(byte[] entries, int pre, int last) {
        for(int i = pre; i < last; i++) db.put(tx, lmdbkey(docid, i), entries);
        dirty();
    }

    @Override
    public void delete(int pre, int nr) {
        if(nr == 0) return;
        for(int i = pre; i < pre+nr; i++) db.delete(tx, lmdbkey(docid, i));
        dirty();
    }

    @Override
    public void insert(int pre, byte[] entries) {
        if (entries.length == 0) return;
        meta.size += (pre + (entries.length >>> IO.NODEPOWER)) - pre;
        db.put(tx, lmdbkey(docid, pre), entries);
        dirty();
    }

    protected byte[] get(int pre) {
        return db.get(tx, lmdbkey(docid, pre));
    }

    protected byte[] get(byte[] key) {
        return db.get(tx,key);
    }

    protected void put(byte[] key, byte[] value) {
        db.put(tx, key, value);
    }
}

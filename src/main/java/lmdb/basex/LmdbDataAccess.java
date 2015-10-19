package lmdb.basex;

import lmdb.util.Byte;
import org.basex.io.IO;
import org.basex.io.random.Buffer;
import org.basex.io.random.Buffers;
import org.fusesource.lmdbjni.Database;
import org.fusesource.lmdbjni.Transaction;

import java.io.Closeable;

import static lmdb.util.Byte.lmdbkey;

public class LmdbDataAccess implements Closeable {

    private final Buffers bm = new Buffers();
    private long length;
    private boolean changed;
    private int off;
    private Database db;
    private byte[] docid;
    private Transaction tx;

    public LmdbDataAccess(final byte[] docid, final Database db, Transaction tx) {
        this.db = db;
        this.docid = docid;
        this.tx = tx;
        length = readLength();
        cursor(0);
    }

    public synchronized void flush() {
        if(tx.isReadOnly()) return;
        for(final Buffer b : bm.all()) if(b.dirty) writeBlock(b);
        if(changed) {
            db.put(tx, getLenKey(docid), Byte.getBytes((int)length));
            changed = false;
        }
    }

    @Override
    public synchronized void close() {
        flush();
    }

    public long cursor() {
        return buffer(false).pos + off;
    }

    public long length() {
        return length;
    }

    public boolean more() {
        return cursor() < length;
    }

    public synchronized byte read1(final long pos) {
        cursor(pos);
        return read1();
    }

    public synchronized byte read1() {
        return (byte) read();
    }

    public synchronized int read4(final long pos) {
        cursor(pos);
        return read4();
    }

    public synchronized int read4() {
        return (read() << 24) + (read() << 16) + (read() << 8) + read();
    }


    public synchronized long read5(final long pos) {
        cursor(pos);
        return read5();
    }

    public synchronized long read5() {
        return ((long) read() << 32) + ((long) read() << 24) + (read() << 16) + (read() << 8) + read();
    }

    public synchronized int readNum(final long p) {
        cursor(p);
        return readNum();
    }

    public synchronized byte[] readToken(final long p) {
        cursor(p);
        return readToken();
    }

    public synchronized byte[] readToken() {
        final int l = readNum();
        return readBytes(l);
    }

    public synchronized byte[] readBytes(final long pos, final int len) {
        cursor(pos);
        return readBytes(len);
    }

    public synchronized byte[] readBytes(final int len) {
        int l = len;
        int ll = IO.BLOCKSIZE - off;
        final byte[] b = new byte[l];

        System.arraycopy(buffer(false).data, off, b, 0, Math.min(l, ll));
        if(l > ll) {
            l -= ll;
            while(l > IO.BLOCKSIZE) {
                System.arraycopy(buffer(true).data, 0, b, ll, IO.BLOCKSIZE);
                ll += IO.BLOCKSIZE;
                l -= IO.BLOCKSIZE;
            }
            System.arraycopy(buffer(true).data, 0, b, ll, l);
        }
        off += l;
        return b;
    }

    public void cursor(final long pos) {
        off = (int) (pos & IO.BLOCKSIZE - 1);
        final long b = pos - off;
        if(!bm.cursor(b)) return;

        final Buffer bf = bm.current();
        if(bf.dirty) writeBlock(bf);
        bf.pos = b;
        if(bf.pos < readLength()) {
            byte[] v = db.get(tx, lmdbkey(docid, (int)(bf.pos/IO.BLOCKSIZE)));
            System.arraycopy(v, 0, bf.data, 0, (int)Math.min(length - bf.pos, IO.BLOCKSIZE));
        }
    }

    public synchronized int readNum() {
        final int value = read();
        switch(value & 0xC0) {
            case 0:
                return value;
            case 0x40:
                return (value - 0x40 << 8) + read();
            case 0x80:
                return (value - 0x80 << 24) + (read() << 16) + (read() << 8) + read();
            default:
                return (read() << 24) + (read() << 16) + (read() << 8) + read();
        }
    }

    public void write5(final long pos, final long value) {
        cursor(pos);
        write((byte) (value >>> 32));
        write((byte) (value >>> 24));
        write((byte) (value >>> 16));
        write((byte) (value >>> 8));
        write((byte) value);
    }

    public void write4(final long pos, final int value) {
        cursor(pos);
        write4(value);
    }

    public void write4(final int value) {
        write(value >>> 24);
        write(value >>> 16);
        write(value >>> 8);
        write(value);
    }

    public void writeNum(final long pos, final int value) {
        cursor(pos);
        writeNum(value);
    }

    public void writeNums(final long p, final int[] values) {
        cursor(p);
        writeNum(values.length);
        for(final int n : values) writeNum(n);
    }

    public void writeBytes(final byte[] buffer, final int offset, final int len) {
        final int last = offset + len;
        int o = offset;

        while(o < last) {
            final Buffer bf = buffer();
            final int l = Math.min(last - o, IO.BLOCKSIZE - off);
            System.arraycopy(buffer, o, bf.data, off, l);
            bf.dirty = true;
            off += l;
            o += l;
            // adjust file size
            final long nl = bf.pos + off;
            if(nl > length) length(nl);
        }
    }

    public void writeToken(final long pos, final byte[] values) {
        cursor(pos);
        writeToken(values, 0, values.length);
    }

    public long free(final long pos, final int size) {
        // old text size (available space)
        int os = readNum(pos) + (int) (cursor() - pos);

        // extend available space by subsequent zero-bytes
        cursor(pos + os);
        for(; pos + os < length && os < size && read() == 0xFF; os++);

        long o = pos;
        if(pos + os == length) {
            // entry is placed last: reset file length (discard last entry)
            length(pos);
        } else {
            int t = size;
            if(os < size) {
                // gap is too small for new entry...
                // reset cursor to overwrite entry
                cursor(pos);
                t = 0;
                // place new entry after last entry
                o = length;
            } else {
                // gap is large enough: set cursor to overwrite remaining bytes
                cursor(pos + size);
            }
            // fill gap with 0xFF for future updates
            while(t++ < os) write(0xFF);
        }
        return o;
    }

    private synchronized void length(final long len) {
        if(len != length) {
            changed = true;
            length = len;
        }
    }

    private int read() {
        final Buffer bf = buffer();
        return bf.data[off++] & 0xFF;
    }

    private void write(final int value) {
        final Buffer bf = buffer();
        bf.dirty = true;
        bf.data[off++] = (byte) value;
        final long nl = bf.pos + off;
        if(nl > length) length(nl);
    }

    private void writeToken(final byte[] buffer, final int offset, final int len) {
        writeNum(len);
        writeBytes(buffer, offset, len);
    }

    private void writeNum(final int value) {
        if(value < 0 || value > 0x3FFFFFFF) {
            write(0xC0); write(value >>> 24); write(value >>> 16); write(value >>> 8); write(value);
        } else if(value > 0x3FFF) {
            write(value >>> 24 | 0x80); write(value >>> 16);
            write(value >>> 8); write(value);
        } else if(value > 0x3F) {
            write(value >>> 8 | 0x40); write(value);
        } else {
            write(value);
        }
    }

    private void writeBlock(final Buffer buffer) {
        if(tx.isReadOnly()) return;
        final long pos = buffer.pos, len = Math.min(IO.BLOCKSIZE, length - pos);
        byte[] v = new byte[(int)len];
        System.arraycopy(buffer.data, 0, v, 0, (int)len);
        db.put(tx, lmdbkey(docid, (int)(pos/IO.BLOCKSIZE)), v);
        buffer.dirty = false;
    }

    private Buffer buffer() {
        return buffer(off == IO.BLOCKSIZE);
    }


    private Buffer buffer(final boolean next) {
        if(next) cursor(bm.current().pos + IO.BLOCKSIZE);
        return bm.current();
    }

    private long readLength() {
        return Byte.getInt(db.get(tx,getLenKey(docid)));
    }

    static byte[] getLenKey(byte[] did) {
        return new byte[] {
                did[0],
                did[1],
                did[2],
                did[3],
                (byte)(0xff),
                (byte)(0xff),
                (byte)(0xff),
                (byte)(0xff)
        };
    }
}

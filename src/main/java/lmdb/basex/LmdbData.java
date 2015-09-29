package lmdb.basex;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.basex.core.MainOptions;
import org.basex.data.Data;
import org.basex.data.Namespaces;
import org.basex.index.IndexType;
import org.basex.index.name.Names;
import org.basex.index.path.PathSummary;
import org.basex.io.IOContent;
import org.basex.io.in.DataInput;
import org.basex.io.out.DataOutput;
import org.basex.util.Token;
import org.basex.util.Util;
import org.fusesource.lmdbjni.Database;
import org.fusesource.lmdbjni.Transaction;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static lmdb.util.Byte.lmdbkey;


public class LmdbData extends Data {

    protected Transaction tx;

    protected Database txtdb;
    protected Database attdb;
    protected Database structdb;

    protected byte[] docid;

    protected LmdbData(final String name, final MainOptions options) {
        super(new MetaData(name, options, null));
    }

    public LmdbData(final String name, final byte[] docid, final Database txtdb, final Database attdb,
                    final Database structdb, final Database tableAccess, final Transaction tx, final MainOptions options) throws IOException {

        super(new MetaData(name, options, null));

        this.docid = docid;
        this.tx = tx;
        this.txtdb = txtdb;
        this.attdb = attdb;
        this.structdb = structdb;

        readStruct();

        this.table = new TableLmdbAccess(meta, tx, tableAccess, docid);

//        if(meta.updindex) idmap = new IdPreMap(meta.lastid); // TODO: check DiskData on old basex-kaha
    }

    @Override
    public void unpin() {

    }

    @Override
    public void close() {
        try {
            table.close();
        } catch(final IOException ex) {
            Util.stack(ex);
        }
        if(tx.isReadOnly()) return;
        writeStruct();
    }

    @Override
    public void createIndex(IndexType type, MainOptions options) throws IOException {

    }

    @Override
    public void dropIndex(IndexType type) throws IOException {

    }

    @Override
    public void startUpdate(MainOptions opts) throws IOException {
    }

    @Override
    public void finishUpdate(MainOptions opts) {
        writeStruct();
    }

    @Override
    public void flush(boolean all) {
        if(tx.isReadOnly()) return;
        try {
            table.flush(all);
        } catch (IOException e) {
            Util.stack(e);
        }
    }

    @Override
    public byte[] text(int pre, boolean text) {
        return (text ? txtdb : attdb).get(tx, lmdbkey(docid, (int)textRef(pre)));
    }

    @Override
    public long textItr(int pre, boolean text) { return Token.toLong(text(pre, text)); }

    @Override
    public double textDbl(int pre, boolean text) { return Token.toDouble(text(pre, text)); }

    @Override
    public int textLen(int pre, boolean text) { return text(pre, text).length; }

    @Override
    public boolean inMemory() { return false; }

    @Override
    protected void updateText(int pre, byte[] value, int kind) {
        (kind != ATTR ? txtdb : attdb).put(tx, lmdbkey(docid, (int) textRef(pre)), value);
    }

    @Override
    protected void delete(int pre, boolean text) {
        (text ? txtdb : attdb).delete(tx, lmdbkey(docid, (int)textRef(pre)));
    }

    @Override
    protected long textRef(byte[] value, boolean text) {
        return 0;
    }

    private void readStruct() throws IOException {

        DataInputStream structin = new DataInputStream(new ByteArrayInputStream(structdb.get(tx,docid)));

        byte[] metastruct = new byte[structin.readInt()];
        structin.readFully(metastruct);
        meta.read(new DataInput(new IOContent(metastruct)));

        byte[] pathstruct = new byte[structin.readInt()];
        structin.readFully(pathstruct);
        paths = new PathSummary(this, new DataInput(new IOContent(pathstruct)));

        byte[] nspacestruct = new byte[structin.readInt()];
        structin.readFully(nspacestruct);
        nspaces = new Namespaces(new DataInput(new IOContent(nspacestruct)));

        byte[] elementstruct = new byte[structin.readInt()];
        structin.readFully(elementstruct);
        elemNames = new Names(new DataInput(new IOContent(elementstruct)),meta);

        byte[] attrstruct = new byte[structin.readInt()];
        structin.readFully(attrstruct);
        attrNames = new Names(new DataInput(new IOContent(attrstruct)),meta);
    }

    private void writeStruct() {

        try(ByteArrayOutputStream bos = new ByteArrayOutputStream(1024*32);
            DataOutputStream dos = new DataOutputStream(bos);
            ByteArrayOutputStream b = new ByteArrayOutputStream(1024*8)) {

            meta.dirty = false;
            meta.write(new DataOutput(b));
            dos.writeInt(b.size());
            dos.write(b.toByteArray());

            b.reset();
            paths.write(new DataOutput(b));
            dos.writeInt(b.size());
            dos.write(b.toByteArray());

            b.reset();
            nspaces.write(new DataOutput(b));
            dos.writeInt(b.size());
            dos.write(b.toByteArray());

            b.reset();
            elemNames.write(new DataOutput(b));
            dos.writeInt(b.size());
            dos.write(b.toByteArray());

            b.reset();
            attrNames.write(new DataOutput(b));
            dos.writeInt(b.size());
            dos.write(b.toByteArray());

            structdb.put(tx, docid, bos.toByteArray());

        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }
}

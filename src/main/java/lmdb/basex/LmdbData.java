package lmdb.basex;

import lmdb.util.Byte;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.basex.core.MainOptions;
import org.basex.core.StaticOptions;
import org.basex.data.Data;
import org.basex.data.Namespaces;
import org.basex.index.IdPreMap;
import org.basex.index.IndexType;
import org.basex.index.ft.FTBuilder;
import org.basex.index.name.Names;
import org.basex.index.path.PathSummary;
import org.basex.io.IOContent;
import org.basex.io.in.DataInput;
import org.basex.io.out.DataOutput;
import org.basex.util.Token;
import org.basex.util.Util;
import org.fusesource.lmdbjni.Transaction;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;

import static lmdb.basex.LmdbDataManager.attributevaldb;
import static lmdb.basex.LmdbDataManager.structdb;
import static lmdb.basex.LmdbDataManager.textdatadb;
import static lmdb.util.Byte.lmdbkey;

public class LmdbData extends Data implements AutoCloseable {

    public static final byte[] LAST_REF_KEY = new byte[] {0,0,0,0};

    protected Transaction tx;

    byte[] docid;

    private volatile int lastTxtRef;
    private volatile int lastAttRef;

    protected LmdbData(final String name, final MainOptions options) {
        super(new LmdbMetaData(name, options, null));
    }

    public LmdbData(final String name, final byte[] docid, final Transaction tx, final MainOptions options,
                    final StaticOptions sopts, final boolean openIndex) throws IOException {

        super(new LmdbMetaData(name, options, sopts));

        this.docid = docid;
        this.tx = tx;

        readStruct();
        initLastRefs();

        this.table = new TableLmdbAccess(meta, tx, docid);

        if(openIndex) {
            textIndex = new UpdatableLmdbValues(this, true, docid, tx);
            attrIndex = new UpdatableLmdbValues(this, false, docid, tx);
        }
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
        if(textIndex != null) textIndex.close();
        if(attrIndex != null) attrIndex.close();
        if(ftxtIndex != null) ftxtIndex.close();
        if(tx.isReadOnly()) return;
        writeStruct();
        writeLastRefs();
    }

    @Override
    public void createIndex(IndexType type, MainOptions options) throws IOException {
        dropIndex(type);
        meta.dirty = true;
        switch(type) {
            case TEXT:
                new LmdbValuesBuilder(docid, this, options, true).build();
                break;
            case ATTRIBUTE:
                new LmdbValuesBuilder(docid, this, options, false).build();
                break;
            case FULLTEXT:
                new LmdbFTBuilder(docid, this, options).build();
                break;
            default:
                throw new IOException("unknown index type while crating index");
        }
    }

    @Override
    public void dropIndex(IndexType type) throws IOException {
        // TODO: basex-lmdb: launch background thread for this?
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
        writeStruct();
        writeLastRefs();
    }

    @Override
    public byte[] text(int pre, boolean text) {
        return (text ? textdatadb : attributevaldb).get(tx, lmdbkey(docid, (int) textRef(pre)));
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
        (kind != ATTR ? textdatadb : attributevaldb).put(tx, lmdbkey(docid, (int) textRef(pre)), value);
    }

    @Override
    protected void delete(int pre, boolean text) {
        (text ? textdatadb : attributevaldb).delete(tx, lmdbkey(docid, (int) textRef(pre)));
    }

    @Override
    protected long textRef(byte[] value, boolean text) {
        (text ? textdatadb : attributevaldb).put(tx, lmdbkey(docid, (text ? ++lastTxtRef : ++lastAttRef)), value);
        return (text ? lastTxtRef : lastAttRef);
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

        try {
            byte[] idpmap = new byte[structin.readInt()];
            structin.readFully(idpmap);
            idmap = new IdPreMap(new DataInput(new IOContent(idpmap)));
        } catch(EOFException eofe) {
            idmap = new IdPreMap(meta.lastid);
        }
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

            b.reset();
            idmap.write(new DataOutput(b));
            dos.writeInt(b.size());
            dos.write(b.toByteArray());

            structdb.put(tx, docid, bos.toByteArray());

        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    private synchronized void initLastRefs() {
        lastTxtRef = Byte.getInt(textdatadb.get(tx, LmdbData.LAST_REF_KEY));
        lastAttRef = Byte.getInt(attributevaldb.get(tx, LmdbData.LAST_REF_KEY));
    }

    private void writeLastRefs() {
        textdatadb.put(tx, LmdbData.LAST_REF_KEY, lmdb.util.Byte.getBytes(lastTxtRef));
        attributevaldb.put(tx, LmdbData.LAST_REF_KEY, lmdb.util.Byte.getBytes(lastAttRef));
    }
}

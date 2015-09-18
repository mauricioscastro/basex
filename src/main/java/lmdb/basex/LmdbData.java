package lmdb.basex;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.basex.core.MainOptions;
import org.basex.data.Data;
import lmdb.basex.MetaData;
import org.basex.data.Namespaces;
import org.basex.index.IndexType;
import org.basex.index.name.Names;
import org.basex.index.path.PathSummary;
import org.basex.io.IOContent;
import org.basex.io.in.DataInput;
import org.basex.io.out.DataOutput;
import org.basex.util.Token;
import org.fusesource.lmdbjni.Database;
import org.fusesource.lmdbjni.Transaction;

import java.io.IOException;

import static lmdb.util.Byte.lmdbkey;


public class LmdbData extends Data {

    protected Transaction tx;

    protected Database txtdb;
    protected Database attdb;
    protected Database metadatadb;
    protected Database elementdb;
    protected Database attributedb;
    protected Database pathsdb;
    protected Database namespacedb;

    protected byte[] docid;

    protected LmdbData(final String name, final MainOptions options) {
        super(new MetaData(name, options, null));
    }

    public LmdbData(final String name, final byte[] docid, final Database txtdb, final Database attdb,
                    final Database metadatadb, final Database elementdb, final Database attributedb, final Database pathsdb,
                    final Database namespacedb, final Database tableAccess, final Transaction tx, final MainOptions options) throws IOException {

        super(new MetaData(name, options, null));

        byte[] dbmeta = metadatadb.get(tx,docid);
        if(dbmeta != null) meta.read(new DataInput(new IOContent(dbmeta)));

        this.docid = docid;
        this.tx = tx;
        this.txtdb = txtdb;
        this.attdb = attdb;
        this.table = new TableLmdbAccess(meta,tx,tableAccess,docid);
        this.metadatadb = metadatadb;
        this.elementdb = elementdb;
        this.attributedb = attributedb;
        this.pathsdb = pathsdb;
        this.namespacedb = namespacedb;

        this.paths = new PathSummary(this, new DataInput(new IOContent(pathsdb.get(tx,docid))));
        this.elemNames = new Names(new DataInput(new IOContent(elementdb.get(tx,docid))),meta);
        this.attrNames = new Names(new DataInput(new IOContent(attributedb.get(tx,docid))),meta);
        this.nspaces = new Namespaces(new DataInput(new IOContent(namespacedb.get(tx,docid))));

//        if(meta.updindex) idmap = new IdPreMap(meta.lastid); // TODO: check DiskData on old basex-kaha
    }

    @Override
    public void unpin() {

    }

    @Override
    public void close() {

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
        System.err.println(elemNames.toString());
        ByteArrayOutputStream bos = new ByteArrayOutputStream(1024*16);

        try {
            paths.write(new DataOutput(bos));
            pathsdb.put(tx, docid, bos.toByteArray());

            bos.reset();
            meta.write(new DataOutput(bos));
            metadatadb.put(tx, docid, bos.toByteArray());

            bos.reset();
            if (nspaces.isEmpty()) nspaces.add(0, new byte[]{0}, new byte[]{0}, this);
            nspaces.write(new DataOutput(bos));
            namespacedb.put(tx, docid, bos.toByteArray());

            bos.reset();
            elemNames.write(new DataOutput(bos));
            elementdb.put(tx, docid, bos.toByteArray());

            bos.reset();
            attrNames.write(new DataOutput(bos));
            attributedb.put(tx, docid, bos.toByteArray());
        } catch (IOException ioe) {

        }

        tx.commit();
    }

    @Override
    public void flush(boolean all) {

    }

    @Override
    public byte[] text(int pre, boolean text) {
        return (text ? txtdb : attdb).get(tx, lmdbkey(docid, pre));
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
        put(pre, value, kind != ATTR);
    }

    @Override
    protected void delete(int pre, boolean text) {
        (text ? txtdb : attdb).delete(tx, lmdbkey(docid, pre));
    }

    @Override
    protected long textRef(byte[] value, boolean text) {
        return 0;
    }

    private void put(int pre, byte[] value, boolean text) {
        (text ? txtdb : attdb).put(tx, lmdbkey(docid, pre), value);
    }
}

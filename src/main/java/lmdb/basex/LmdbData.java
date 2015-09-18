package lmdb.basex;

import org.basex.core.MainOptions;
import org.basex.data.Data;
import lmdb.basex.MetaData;
import org.basex.data.Namespaces;
import org.basex.index.IndexType;
import org.basex.index.name.Names;
import org.basex.index.path.PathSummary;
import org.basex.io.IOContent;
import org.basex.io.in.DataInput;
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
        byte[] dben = elementdb.get(tx,docid);
        this.elemNames = dben == null ? new Names(meta) : new Names(new DataInput(new IOContent(dben)),meta);
        byte[] dban = attributedb.get(tx,docid);
        this.attrNames = dban == null ? new Names(meta) : new Names(new DataInput(new IOContent(dban)),meta);
        byte[] dbns = namespacedb.get(tx,docid);
        this.nspaces = dbns == null ? new Namespaces() : new Namespaces(new DataInput(new IOContent(dbns)));

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

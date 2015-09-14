package lmdb.basex;

import org.basex.core.MainOptions;
import org.basex.data.Data;
import org.basex.data.MetaData;
import org.basex.data.Namespaces;
import org.basex.index.IdPreMap;
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
    protected Database elementdb;
    protected Database attributedb;
    protected Database pathsdb;
    protected Database namespacedb;

    protected byte[] docid;


    protected LmdbData(final String name, final MainOptions options) {
        super(new MetaData(name, options, null));
    }

    public LmdbData(final String name, final byte[] docid, final Database elemNames,
                    final Database attrNames, final Database paths, final Database nspaces,
                    final Database tableAccess, final Transaction tx, final MainOptions options) throws IOException {

        super(new MetaData(name, options, null));
        this.tx = tx;
        table = new TableLmdbAccess(meta,tx,tableAccess,docid);
        elementdb = elemNames;
        attributedb = attrNames;
        pathsdb = paths;
        namespacedb = nspaces;
//        if(meta.updindex) idmap = new IdPreMap(meta.lastid); // TODO: check DiskData on old basex-kaha
        this.elemNames = new Names(new DataInput(new IOContent(elemNames.get(docid))),meta);
        this.attrNames = new Names(new DataInput(new IOContent(attrNames.get(docid))),meta);
        this.paths = new PathSummary(this, new DataInput(new IOContent(paths.get(docid))));
        this.nspaces = new Namespaces(new DataInput(new IOContent(nspaces.get(docid))));
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
        put(pre, value, kind == TEXT);
    }

    @Override
    protected void delete(int pre, boolean text) {
        (text ? txtdb : attdb).delete(tx, lmdbkey(docid, pre));
    }

    @Override
    protected long textRef(byte[] value, boolean text) { return -1; }


    protected void put(int pre, byte[] value, boolean text) {
        (text ? txtdb : attdb).put(tx, lmdbkey(docid, pre), value);
    }
}

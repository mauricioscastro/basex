package lmdb.basex;

import org.basex.build.Builder;
import org.basex.build.Parser;
import org.basex.data.Data;
import org.basex.data.DataClip;
import org.fusesource.lmdbjni.Database;
import org.fusesource.lmdbjni.Env;

import java.io.IOException;

import static org.fusesource.lmdbjni.Constants.bytes;

public class LmdbBuilder extends Builder {

    private LmdbDataBuilder data;

    private LmdbBuilder(final String name, final byte[] docid, final Env env,
                        final Database txtdb, final Database attdb,
                        final Database elemNames, final Database attrNames, final Database paths, final Database nspaces,
                        final Database tableAccess, final Parser parser) throws IOException {

        super(name, parser);
        data = new LmdbDataBuilder(name, docid, env, txtdb, attdb, elemNames, attrNames, paths, nspaces, tableAccess, parser.options);
    }

    public static void build(final String name, final byte[] docid, final Env env, final Database txtdb, final Database attdb,
                             final Database elemNames, final Database attrNames,
                             final Database paths, final Database nspaces,
                             final Database tableAccess, final Parser parser) throws IOException {
        new LmdbBuilder(name, docid, env, txtdb, attdb, elemNames, attrNames, paths, nspaces, tableAccess, parser).build();
    }

    @Override
    public LmdbData build() throws IOException {
        meta = data.meta;
        meta.name = dbname;
        elemNames = data.elemNames;
        attrNames = data.attrNames;
        path.data(data);
        dataClip();
        return null;
    }

    @Override
    public DataClip dataClip() throws IOException {
//        meta.assign(parser);
        try {
            parse();
        } catch(final IOException ex) {
            try { close(); } catch(final IOException ignored) { }
            throw ex;
        }
        close();
        return new DataClip(data);
    }

    @Override
    public void close() throws IOException {
        data.close();
        parser.close();
    }


    @Override
    protected void addDoc(byte[] value) throws IOException {
        data.doc(0, bytes(dbname));
        data.insert(meta.size);
    }

    @Override
    protected void addElem(int dist, int nameId, int asize, int uriId, boolean ne) throws IOException {
        data.elem(dist, nameId, asize, asize, uriId, ne);
        data.insert(meta.size);
    }

    @Override
    protected void addAttr(int nameId, byte[] value, int dist, int uriId) throws IOException {
        data.attr(dist, nameId, value, uriId);
        data.insert(meta.size);
    }

    @Override
    protected void addText(byte[] value, int dist, byte kind) throws IOException {
        data.text(dist, value, kind);
        data.insert(meta.size);
    }

    @Override
    protected void setSize(int pre, int size) throws IOException { data.size(pre, Data.ELEM, size); }
}

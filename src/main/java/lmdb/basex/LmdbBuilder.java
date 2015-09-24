package lmdb.basex;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.basex.build.Builder;
import org.basex.build.Parser;
import org.basex.core.MainOptions;
import org.basex.core.StaticOptions;
import org.basex.data.Data;
import org.basex.data.DataClip;
import org.basex.data.DiskData;
import org.basex.index.name.Names;
import org.basex.io.IO;
import org.basex.io.IOFile;
import org.basex.io.in.DataInput;
import org.basex.io.out.DataOutput;
import org.basex.io.out.TableOutput;
import org.basex.io.random.TableAccess;
import org.basex.io.random.TableDiskAccess;
import org.basex.util.Util;
import org.fusesource.lmdbjni.Database;
import org.fusesource.lmdbjni.Env;
import org.fusesource.lmdbjni.Transaction;

import org.apache.commons.io.IOUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;


import static lmdb.util.Byte.lmdbkey;
import static org.basex.data.DataText.DATATBL;
import static org.basex.data.DataText.DATATMP;
import static java.nio.charset.StandardCharsets.UTF_8;

import static org.basex.core.StaticOptions.DBPATH;

public class LmdbBuilder extends Builder {

    private DataOutput tout;
    private DataOutput sout;
    private StaticOptions sopts;
    private boolean closed;

    private Env env;
    private Database txtdb;
    private Database attdb;
    private Database structdb;
    private Database tableAccess;
    private byte[] docid;
    private DataOutputStream tempBuffer;
    private File tmpFile;

    private LmdbBuilder(final String name, final byte[] docid, final Env env,
                        final Database txtdb, final Database attdb, final Database structdb,
                        final Database tableAccess, final Parser parser,
                        final MainOptions opts, final StaticOptions sopts) throws IOException {

        super(name, parser);
        this.sopts = sopts;
        sopts.set(DBPATH, System.getProperty("java.io.tmpdir", "/tmp"));
        meta = new MetaData(name, opts, sopts);

        this.docid = docid;
        this.env = env;
        this.txtdb = txtdb;
        this.attdb = attdb;
        this.structdb = structdb;
        this.tableAccess = tableAccess;

        this.tmpFile = File.createTempFile("txt." + meta.name.replace('/', '.') + ".", ".tmp", null);
        this.tmpFile.deleteOnExit();
        this.tempBuffer = new DataOutputStream(new FileOutputStream(tmpFile));
    }


    public static LmdbData build(final String name, final byte[] docid, final Env env,
                             final Database txtdb, final Database attdb, final Database structdb,
                             final Database tableAccess, final Parser parser,
                             final MainOptions opts, final StaticOptions sopts) throws IOException {
        return new LmdbBuilder(name, docid, env, txtdb, attdb, structdb, tableAccess, parser, opts, sopts).build();
    }

    @Override
    public LmdbData build() throws IOException {
        meta.assign(parser);
        meta.dirty = true;

        // calculate optimized output buffer sizes to reduce disk fragmentation
        final Runtime rt = Runtime.getRuntime();
        final long max = Math.min(1 << 22, rt.maxMemory() - rt.freeMemory() >> 2);
        int bs = (int) Math.min(meta.filesize, max);
        bs = Math.max(IO.BLOCKSIZE, bs - bs % IO.BLOCKSIZE);

        String tblBaseName = sopts.get(DBPATH)+"/"+meta.name.replace('/','.')+".tbl";
        String tblTmpName =  sopts.get(DBPATH)+"/"+meta.name.replace('/','.')+".tmp";

        elemNames = new Names(meta);
        attrNames = new Names(meta);
        try {
            tout = new DataOutput(new LmdbTableOutput((MetaData)meta, tblBaseName, tableAccess, docid));
            sout = new DataOutput(new IOFile(tblTmpName), bs);
            parse();
        } catch(final IOException ex) {
            try { close(); } catch(final IOException ignored) { }
            throw ex;
        }
        close();

        int p = 0;
        Transaction tx = env.createWriteTransaction();
        FileInputStream tbl = new FileInputStream(tblBaseName);
        for(int i = 0; ; i++) {
            byte[] b = new byte[IO.BLOCKSIZE];
            try { IOUtils.readFully(tbl,b); } catch(Exception e) { break; }
            tableAccess.put(tx,lmdbkey(docid,i),b);
            if(++p > 1000) {
                tx.commit();
                tx = env.createWriteTransaction();
                p = 0;
            }
        }
        if(p > 0) tx.commit();
        else tx.close();

        p = 0;
        tx = env.createWriteTransaction();
        try(final DataInput in = new DataInput(new IOFile(tblTmpName))) {
            final TableLmdbAccess ta = new TableLmdbAccess(meta, tx, tableAccess, docid);
            try {
                for(; spos < ssize; ++spos) {
                    ta.write4(in.readNum(), 8, in.readNum());
                    if(++p > 1000) {
                        tx.commit();
                        tx = env.createWriteTransaction();
                        ta.setTx(tx);
                        p = 0;
                    }
                }
            } finally {
                ta.close();
            }
            if(p > 0) tx.commit();
            else tx.close();
        }

        FileUtils.deleteQuietly(new File(tblTmpName));
        FileUtils.deleteQuietly(new File(tblBaseName));

        writeTextData();
        writeStruct();

        // just create it. do not use right away
        return null;
    }

    public void abort() {
        try {
            close();
        } catch(final IOException ex) {
            Util.debug(ex);
        }
        //if(meta != null) DropDB.drop(meta.name, sopts);
    }

    @Override
    public DataClip dataClip() throws IOException {
        return new DataClip(build());
    }

    @Override
    public void close() throws IOException {
        if(closed) return;
        closed = true;
        if(tout != null) tout.close();
        if(sout != null) sout.close();
        parser.close();
        tout = null;
        sout = null;
    }

    @Override
    protected void addDoc(final byte[] value) throws IOException {
        tout.write1(Data.DOC);
        tout.write2(0);
//        tout.write5(textRef(value, true));
        tout.write5(textRef(meta.name.getBytes(UTF_8), true));
        tout.write4(0);
        tout.write4(meta.size++);
    }

    @Override
    protected void addElem(final int dist, final int nameId, final int asize, final int uriId,
                           final boolean ne) throws IOException {

        tout.write1(asize << 3 | Data.ELEM);
        tout.write2((ne ? 1 << 15 : 0) | nameId);
        tout.write1(uriId);
        tout.write4(dist);
        tout.write4(asize);
        tout.write4(meta.size++);

    }

    @Override
    protected void addAttr(final int nameId, final byte[] value, final int dist, final int uriId)
            throws IOException {

        tout.write1(dist << 3 | Data.ATTR);
        tout.write2(nameId);
        tout.write5(textRef(value, false));
        tout.write4(uriId);
        tout.write4(meta.size++);
    }

    @Override
    protected void addText(final byte[] value, final int dist, final byte kind) throws IOException {
        tout.write1(kind);
        tout.write2(0);
        tout.write5(textRef(value, true));
        tout.write4(dist);
        tout.write4(meta.size++);
    }

    @Override
    protected void setSize(final int pre, final int size) throws IOException {
        sout.writeNum(pre);
        sout.writeNum(size);
        ++ssize;
    }

    private long textRef(final byte[] value, final boolean text) throws IOException {
        try {
            tempBuffer.writeInt(value.length);
            tempBuffer.write(lmdbkey(docid, meta.size));
            tempBuffer.write(value);
            tempBuffer.writeBoolean(text);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return meta.size;
    }


    private void writeTextData() throws IOException {

            tempBuffer.close();

            Transaction tx = env.createWriteTransaction();

            DataInputStream di = new DataInputStream(new FileInputStream(tmpFile));

            int c = 0;
            try {
                while(true) try {
                    int len = di.readInt();
                    byte[] key = new byte[8];
                    di.readFully(key);
                    byte[] value = new byte[len];
                    di.readFully(value);
                    boolean text = di.readBoolean();
                    (text ? txtdb : attdb).put(tx, key, value);
                    c++;
                    if (c > 1024 * 10) {
                        tx.commit();
                        c = 0;
                        tx = env.createWriteTransaction();
                    }
                } catch (EOFException eofe) {
                    break;
                }
                if (c > 0) tx.commit();
            } finally {
                tx.close();
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
            path.write(new DataOutput(b));
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

            structdb.put(docid, bos.toByteArray());

        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

//    private LmdbDataBuilder data;
//
//    private LmdbBuilder(final String name, final byte[] docid, final Env env,
//                        final Database txtdb, final Database attdb, final Database metadatadb,
//                        final Database elemNames, final Database attrNames, final Database paths, final Database nspaces,
//                        final Database tableAccess, final Parser parser) throws IOException {
//
//        super(name, parser);
//        data = new LmdbDataBuilder(name, docid, env, txtdb, attdb, metadatadb, elemNames, attrNames, paths, nspaces, tableAccess, parser.options);
//    }
//
//    public static void build(final String name, final byte[] docid, final Env env, final Database txtdb, final Database attdb,
//                             final Database metadatadb, final Database elemNames, final Database attrNames,
//                             final Database paths, final Database nspaces,
//                             final Database tableAccess, final Parser parser) throws IOException {
//        new LmdbBuilder(name, docid, env, txtdb, attdb, metadatadb, elemNames, attrNames, paths, nspaces, tableAccess, parser).build();
//    }
//
//    @Override
//    public LmdbData build() throws IOException {
//        meta = data.meta;
//        meta.name = dbname;
//        meta.original = dbname;
//        elemNames = data.elemNames;
//        attrNames = data.attrNames;
//        path.data(data);
//        nspaces = data.nspaces;
//        dataClip();
//        return null;
//    }
//
//    @Override
//    public DataClip dataClip() throws IOException {
//        try {
//            parse();
//        } catch(final IOException ex) {
//            try { close(); } catch(final IOException ignored) { }
//            throw ex;
//        }
//        close();
//        return new DataClip(data);
//    }
//
//    @Override
//    public void close() throws IOException {
//        data.close();
//        parser.close();
//    }
//
//
//    @Override
//    protected void addDoc(byte[] value) throws IOException {
//        data.doc(0, bytes(dbname));
//        data.insert(meta.size);
//    }
//
//    @Override
//    protected void addElem(int dist, int nameId, int asize, int uriId, boolean ne) throws IOException {
//        data.elem(dist, nameId, asize, asize, uriId, ne);
//        data.insert(meta.size);
//    }
//
//    @Override
//    protected void addAttr(int nameId, byte[] value, int dist, int uriId) throws IOException {
//        data.attr(dist, nameId, value, uriId);
//        data.insert(meta.size);
//    }
//
//    @Override
//    protected void addText(byte[] value, int dist, byte kind) throws IOException {
//        data.text(dist, value, kind);
//        data.insert(meta.size);
//    }
//
//    @Override
//    protected void setSize(int pre, int size) throws IOException { data.size(pre, Data.ELEM, size); }
}

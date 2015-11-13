package lmdb.basex;

import lmdb.util.Byte;
import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;
import org.basex.build.xml.XMLParser;
import org.basex.core.MainOptions;
import org.basex.core.StaticOptions;
import org.basex.data.Data;
import org.basex.index.IndexType;
import org.basex.io.IOStream;
import org.basex.util.Util;
import org.fusesource.lmdbjni.Database;
import org.fusesource.lmdbjni.Entry;
import org.fusesource.lmdbjni.EntryIterator;
import org.fusesource.lmdbjni.Env;
import org.fusesource.lmdbjni.Transaction;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static lmdb.Constants.string;
import static org.fusesource.lmdbjni.Constants.FIXEDMAP;
import static org.fusesource.lmdbjni.Constants.bytes;

// TODO: basex-lmdb: cleaner: add zombie entries check+removal in all tables
// TODO: basex-lmdb: cleaner: for each get first and last and check all in the interval against coldb

public class LmdbDataManager {

    private static final Logger logger = Logger.getLogger(LmdbDataManager.class);

    private static String home;

    static Env env = null;
    static Database coldb;
    static Database structdb;
    static Database tableaccessdb;
    static Database textdatadb;
    static Database attributevaldb;
    static Database txtindexldb;
    static Database txtindexrdb;
    static Database attindexldb;
    static Database attindexrdb;
    static Database ftindexxdb;
    static Database ftindexydb;
    static Database ftindexzdb;

    private static volatile boolean cleanerRunning = true;
    private static volatile boolean cleanerStopped = false;

    private static final byte[] LAST_DOCUMENT_INDEX_KEY = new byte[]{0};
    private static final byte[] COLLECTION_LIST_KEY = new byte[]{1};

    public static void config(String home) { config(home, 100); }

    public static void config(String home, long size) {
        if(env != null) return;
        LmdbDataManager.home = home;
        env = new Env();
        env.setMapSize(size*1024000000000L);
        env.setMaxDbs(16);
        env.open(home, FIXEDMAP);
    }

    public static void start(boolean runCleaner) {

        coldb = env.openDatabase("collections");
        structdb = env.openDatabase("structure");
        tableaccessdb = env.openDatabase("table_access");
        textdatadb = env.openDatabase("text_node_data");
        attributevaldb = env.openDatabase("attribute_values");
        txtindexldb = env.openDatabase("txtindexldb");
        txtindexrdb = env.openDatabase("txtindexrdb");
        attindexldb = env.openDatabase("attindexldb");
        attindexrdb = env.openDatabase("attindexrdb");
        ftindexxdb = env.openDatabase("ftindexxdb");
        ftindexydb = env.openDatabase("ftindexydb");
        ftindexzdb = env.openDatabase("ftindexzdb");

        try {
            String[] lc = _listCollections();
            if(lc != null) for(String c: _listCollections()) if(c.endsWith("/r")) removeAllDocuments(c.substring(0,c.indexOf('/')));
        } catch (IOException e) {
            Util.notExpected(e);
        }

        logger.info("start");

        if(runCleaner) {
            new Thread(new Cleaner()).start();
        } else {
            cleanerStopped = true;
        }
    }

    public static void start() {
        start(true);
    }

    public static void stop() {
        cleanerRunning = false;
        while(!cleanerStopped) try { Thread.sleep(500); } catch(InterruptedException ie) {}
        env.sync(true);
        coldb.close();
        structdb.close();
        tableaccessdb.close();
        textdatadb.close();
        attributevaldb.close();
        txtindexldb.close();
        txtindexrdb.close();
        attindexldb.close();
        attindexrdb.close();
        ftindexxdb.close();
        ftindexydb.close();
        ftindexzdb.close();
        env.close();
        logger.info("stop");
    }

    public static synchronized void createCollection(final String name) throws IOException {
        try(Transaction tx = env.createWriteTransaction()) {
            byte[] cl = coldb.get(tx,COLLECTION_LIST_KEY);
            if (cl != null) {
                String collections = string(cl,1,cl.length-2);
                HashSet<String> collection = new HashSet<String>(Arrays.asList(collections.split(", ")));
                if(collection.contains(name)) return;
                collection.add(name);
                coldb.put(tx, COLLECTION_LIST_KEY, bytes(collection.toString()));
            } else {
                coldb.put(tx,COLLECTION_LIST_KEY, bytes("["+name+"]"));
            }
            tx.commit();
        }
    }

    private static String[] _listCollections() throws IOException {
        byte[] cl = coldb.get(COLLECTION_LIST_KEY);
        if (cl == null) return null;
        String[] clist = string(cl,1,cl.length-2).split(", ");
        Arrays.sort(clist);
        return clist;
    }

    public static List<String> listCollections() throws IOException {
        String[] clist = _listCollections();
        if(clist == null) return new ArrayList<String>();
        ArrayList<String> collection = new ArrayList<String>(clist.length);
        for(String c: clist) if(!c.endsWith("/r")) collection.add(c);
        return collection;
    }

    public static synchronized void removeCollection(final String name) throws IOException {
        byte[] cl = coldb.get(COLLECTION_LIST_KEY);
        if (cl == null) return;
        HashSet<String> collection = new HashSet<String>(Arrays.asList(string(cl,1,cl.length-2).split(", ")));
        if(collection.remove(name)) {
            collection.add(name+"/r");
            coldb.put(COLLECTION_LIST_KEY, bytes(collection.toString()));
            removeAllDocuments(name);
        }
    }

    public static void createDocument(final String name, InputStream content) throws IOException {
        byte[] docid = getNextDocumentId(name);
        MainOptions opt = new MainOptions();
        LmdbBuilder.build(name, docid, new XMLParser(new IOStream(content), opt), opt, new StaticOptions(false));
        indexDocument(name);
    }

    public static void indexDocument(final String name) throws IOException {
        MainOptions opt = new MainOptions();
        try(Transaction tx = env.createReadTransaction(); LmdbData data = (LmdbData)openDocument(name, opt, tx, false)) {
            data.createIndex(IndexType.TEXT, opt);
            data.createIndex(IndexType.ATTRIBUTE, opt);
            data.createIndex(IndexType.FULLTEXT, opt);
        }

    }

    public static void dropDocumentIndex(final String name) throws IOException {
        MainOptions opt = new MainOptions();
        try(Transaction tx = env.createReadTransaction(); LmdbData data = (LmdbData)openDocument(name, opt, tx, false)) {
            data.dropIndex(IndexType.TEXT);
            data.dropIndex(IndexType.ATTRIBUTE);
            data.dropIndex(IndexType.FULLTEXT);
        }

    }

    public static List<String> listDocuments(String collection) throws IOException {
        return listDocuments(collection, false);
    }

    public static List<String> listDocuments(String collection, boolean addCollectionName) throws IOException {
        ArrayList<String> docs = new ArrayList<String>();
        try(Transaction tx = env.createReadTransaction(); EntryIterator ei = coldb.seek(tx, bytes(collection))) {
            while (ei.hasNext()) {
                Entry e = ei.next();
                String key = string(e.getKey());
                if(key.endsWith("/r")) continue;
                if(!key.startsWith(collection)) break;
                docs.add(addCollectionName ? key : key.substring(key.indexOf('/')+1));
            }
        }
        return docs;
    }

    public static void removeDocument(final String name) throws IOException {
        byte[] docid = coldb.get(bytes(name));
        if(docid == null) return;
        try(Transaction tx = env.createWriteTransaction()) {
            if(coldb.delete(tx, bytes(name))) coldb.put(tx, bytes(name + "/r"), docid);
            tx.commit();
        }
    }

    public static String home() {
        return home;
    }

    static Data openDocument(String name, MainOptions options, Transaction tx) throws IOException {
        return openDocument(name, options, tx, true);
    }


    private static Data openDocument(String name, MainOptions options, Transaction tx, boolean openIndex) throws IOException {
        byte[] docid = coldb.get(tx,bytes(name));
        if(docid == null) throw new IOException("document " + name + " not found");
        return new LmdbData(name, docid, tx, options, new LmdbStaticOptions(), openIndex);
    }

    private static synchronized byte[] getNextDocumentId(final String name) throws IOException {
        if(coldb.get(bytes(name)) != null) throw new IOException("document " + name + " exists");
        int i = name.indexOf('/');
        if(i > 0 && name.length() > 2) {
            String docname = name.substring(i+1);
            if(docname.indexOf('/') != -1) throw new IOException("document " + docname + " name is malformed");
            String collection = name.substring(0,i);
            if(!listCollections().contains(collection)) throw new IOException("unknown collection " + collection);
        } else {
            throw new IOException("malformed document name " + name +  " or unknown collection. 'collection_name/document_name' needed");
        }
        try(Transaction tx = env.createWriteTransaction()) {
            byte[] docid = coldb.get(tx, LAST_DOCUMENT_INDEX_KEY);
            if(docid == null) {
                docid = new byte[]{0,0,0,0};
            } else {
                Byte.setInt(Byte.getInt(docid)+1,docid);
            }
            coldb.put(tx, LAST_DOCUMENT_INDEX_KEY, docid);
            tx.commit();
            return docid;
        }
    }

    private static synchronized void removeAllDocuments(String collection) {
        try(Transaction tx = env.createReadTransaction(); EntryIterator ei = coldb.seek(tx, bytes(collection))) {
            try(Transaction wtx = env.createWriteTransaction()) {
                while (ei.hasNext()) {
                    Entry e = ei.next();
                    if (!string(e.getKey()).startsWith(collection)) break;
                    if (coldb.delete(wtx, e.getKey())) coldb.put(wtx, bytes(string(e.getKey()) + "/r"), e.getValue());
                }
                wtx.commit();
            }
        }
    }

    private static class Cleaner implements Runnable {

        private int writeBatchSize = 10000;
        private int mincycle = 60; // minutes
        private int secscounter = 0;

        private Database[] dblist =  new Database[]{
                tableaccessdb, textdatadb, attributevaldb, txtindexldb, txtindexrdb,
                attindexldb, attindexrdb, ftindexxdb, ftindexydb, ftindexzdb
        };

        @Override
        public void run() {
            logger.info("cleaner start");
            while(cleanerRunning) try {
                Thread.sleep(1000);
                if(secscounter++ < 60 * mincycle) continue;
                DocRef dr = getNextRemovedDoc();
                while(dr != null) {
                    boolean retry = false;
                    String docName = string(dr.key);
                    logger.info("cleaner: removing document " + docName.substring(0, docName.length() - 2));
                    structdb.delete(dr.ref);
                    for (Database db : dblist) {
                        try (Transaction tx = env.createReadTransaction(); EntryIterator dbei = db.seek(tx, dr.ref)) {
                            int c = 0;
                            Transaction wtx = env.createWriteTransaction();
                            try {
                                while (dbei.hasNext()) {
                                    byte[] key = dbei.next().getKey();
                                    if (Byte.getInt(dr.ref) != Byte.getInt(key)) break;
                                    db.delete(wtx, key);
                                    if (++c > writeBatchSize) {
                                        wtx.commit();
                                        wtx = env.createWriteTransaction();
                                        c = 0;
                                    }
                                }
                            } catch (Exception e) {
                                retry = true;
                            } finally {
                                if(c > 0) wtx.commit();
                                else wtx.close();
                            }
                        } catch (Exception e) {
                            retry = true;
                        }
                    }
                    if(!retry) coldb.delete(dr.key);
                    dr = getNextRemovedDoc();
                }
                secscounter = 0;
            } catch(InterruptedException ie) {
                break;
            }
            logger.info("cleaner stop");
            cleanerStopped = true;
        }

        private DocRef getNextRemovedDoc() {
            try(Transaction tx = env.createReadTransaction(); EntryIterator coldbei = coldb.iterate(tx)) {
                while(coldbei.hasNext()) {
                    Entry e = coldbei.next();
                    if(string(e.getKey()).endsWith("/r")) return new DocRef(e.getKey(),e.getValue());
                }
            }
            return null;
        }

        private class DocRef {
            public byte[] key;
            public byte[] ref;
            public DocRef(byte[] k, byte[] v) {
                key = k;
                ref = v;
            }
        }
    }
}

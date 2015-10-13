package lmdb.basex;

import lmdb.util.Byte;
import org.apache.log4j.Logger;
import org.basex.build.xml.XMLParser;
import org.basex.core.MainOptions;
import org.basex.core.StaticOptions;
import org.basex.data.Data;
import org.basex.io.IOStream;
import org.basex.util.Util;
import org.fusesource.lmdbjni.Database;
import org.fusesource.lmdbjni.Entry;
import org.fusesource.lmdbjni.EntryIterator;
import org.fusesource.lmdbjni.Env;
import org.fusesource.lmdbjni.Transaction;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static lmdb.Constants.string;
import static org.fusesource.lmdbjni.Constants.FIXEDMAP;
import static org.fusesource.lmdbjni.Constants.bytes;

// TODO: basex-lmdb: cleaner: add zombie entries check+removal in all tables fora each get first and last and check against coldb

public class LmdbDataManager {

    private static final Logger logger = Logger.getLogger(LmdbDataManager.class);

    static Env env = null;
    private static String home;
    private static Database coldb;
    private static Database structdb;
    private static Database tableaccessdb;
    private static Database textdatadb;
    private static Database attributevaldb;
    private static Database txtindexldb;
    private static Database txtindexrdb;
    private static Database attindexldb;
    private static Database attindexrdb;
    private static Database ftindexxdb;
    private static Database ftindexydb;
    private static Database ftindexzdb;

    private static volatile boolean cleanerRunning = true;
    private static volatile boolean cleanerStopped = false;

    private static final byte[] LAST_DOCUMENT_INDEX_KEY = new byte[]{0};
    private static final byte[] COLLECTION_LIST_KEY = new byte[]{1};

    public static void config(String home) {
        config(home, 102400000000000l);
    }

    public static void config(String home, long size) {
        if(env != null) return;
        LmdbDataManager.home = home;
        env = new Env();
        env.setMapSize(size);
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
        LmdbBuilder.build(name, docid, env, coldb, textdatadb, attributevaldb, structdb, tableaccessdb,
                          new XMLParser(new IOStream(content),opt), opt, new StaticOptions(false));
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
        byte[] docid = coldb.get(tx,bytes(name));
        if(docid == null) throw new IOException("document " + name + " not found");
        return new LmdbData(name, docid, textdatadb, attributevaldb, structdb, tableaccessdb, tx, options);
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
        private int mincycle = 10; // minutes
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

    public static final String CONTENT = "\n" +
            "<root xmlns:h=\"http://www.w3.org/TR/html4/\"\n" +
            "xmlns:f=\"http://www.w3schools.com/furniture\">\n" +
            "<h:table border=\"1\" cellspacing=\"2\">\n" +
            "  <h:tr>\n" +
            "    <h:td width=\"100%\">Apples</h:td>\n" +
            "    <h:td>Bananas</h:td>\n" +
            "  </h:tr>\n" +
            "</h:table>\n" +
            "<f:table>\n" +
            "  <f:name>African Coffee Table</f:name>\n" +
            "  <f:width>80</f:width>\n" +
            "  <f:length>120</f:length>\n" +
            "</f:table>\n" +
            "<f:table>\n" +
            "  <f:name>African Coffee Table</f:name>\n" +
            "  <f:width>80</f:width>\n" +
            "  <f:length>120</f:length>\n" +
            "</f:table>\n" +
            "<f:table>\n" +
            "  <f:name>African Coffee Table</f:name>\n" +
            "  <f:width>80</f:width>\n" +
            "  <f:length>120</f:length>\n" +
            "</f:table>\n" +
            "<f:table cellspacing=\"0\">\n" +
            "  <f:name>African Coffee Table</f:name>\n" +
            "  <f:width>80</f:width>\n" +
            "  <f:length>120</f:length>\n" +
            "</f:table>\n" +
            "<f:table>\n" +
            "  <f:name>African Coffee Table</f:name>\n" +
            "  <f:width>80</f:width>\n" +
            "  <f:length>120</f:length>\n" +
            "</f:table>\n" +
            "<f:table>\n" +
            "  <f:name>African Coffee Table</f:name>\n" +
            "  <f:width>80</f:width>\n" +
            "  <f:length>120</f:length>\n" +
            "</f:table>\n" +
            "<empty att1='oi'/>\n" +
            "<not_empty><x/></not_empty>\n" +
            "</root> ";


    public static void main(String[] arg) throws Exception {

        String home = "/home/mscastro/dev/basex-lmdb/db";
        MainOptions opt = new MainOptions();
        opt.set(MainOptions.XMLPATH, home + "/xml");
        opt.set(MainOptions.MODPATH, home + "/module");
        LmdbDataManager.config(home);
        LmdbDataManager.start();

//        LmdbDataManager.createCollection("c1");
//        LmdbDataManager.createCollection("c2");
//        LmdbDataManager.createCollection("c3");
//        LmdbDataManager.removeCollection("c1");
//        LmdbDataManager.createCollection("c1");
//        LmdbDataManager.removeCollection("c1");
//        LmdbDataManager.createCollection("c4");
//        LmdbDataManager.createDocument("c4/d0", new ByteArrayInputStream(CONTENT.getBytes()));
//        LmdbDataManager.createDocument("c4/d1", new FileInputStream("/home/mscastro/dev/basex-lmdb/db/xml/etc/factbook.xml"));
//        LmdbDataManager.createDocument("c4/d2", new FileInputStream("/home/mscastro/download/shakespeare.xml"));
//        LmdbDataManager.createDocument("c4/d3", new FileInputStream("/home/mscastro/download/medline15n0766.xml"));
//        LmdbDataManager.createDocument("c4/d4", new FileInputStream("/home/mscastro/download/standard.xml"));
//        LmdbDataManager.createDocument("c4/d5", new FileInputStream("/tmp/test.xml"));

//        LmdbDataManager.createDocument("c1/d1", new FileInputStream("/home/mscastro/download/shakespeare/tempest.xml"));
//        LmdbDataManager.createDocument("c1/d2", new FileInputStream("/home/mscastro/download/shakespeare/coriolan.xml"));
//        LmdbDataManager.createDocument("c1/d3", new FileInputStream("/home/mscastro/download/shakespeare/all_well.xml"));

//        LmdbDataManager.createDocument("c2/d0", new ByteArrayInputStream(new byte[]{}));

//        System.out.println(LmdbDataManager.listDocuments("c4"));

//        LmdbDataManager.removeCollection("c1");

//        LmdbDataManager.removeDocument("c1/d2");

//        System.out.println(LmdbDataManager.listDocuments("c4"));

//        System.out.println(LmdbDataManager.listCollections());

// -----------------------------------------------------------------------------------------------------------------------

//        LmdbDataManager.t();

        try(LmdbQueryContext ctx = new LmdbQueryContext("doc('file://etc/books.xml')", opt)) {
            ctx.run(System.out);
        }

//        try(LmdbQueryContext ctx = new LmdbQueryContext("insert node <new_element_a name='a'/> into doc('c4/d0')/root")) {
//            ctx.run(System.out);
//        }
//
//        try(LmdbQueryContext ctx = new LmdbQueryContext("doc('c4/d0')")) {
//            ctx.run(System.out);
//        }

//
//        try(Transaction tx = env.createReadTransaction()) {
//            EntryIterator ei = coldb.iterate(tx);
//            while (ei.hasNext()) {
//                Entry e = ei.next();
//                System.err.println("coldb: " + string(e.getKey()) + ":" + Hex.encodeHexString(e.getValue()));
//            }
//        }


        //System.err.println(Hex.encodeHexString(key(10, 11)));

//        try(Transaction tx = env.createWriteTransaction()) {
//
//            EntryIterator ei = coldb.seek(tx, key(10,0));
//            while (ei.hasNext()) {
//                Entry e = ei.next();
//                if(Byte.getInt(e.getKey()) != 10) break;
//                System.err.println(Hex.encodeHexString(e.getKey()) + ":" + string(e.getValue()));
//            }
//            tx.commit();
//        }

//        for(int i = 0; i < 100000; i++) {
//            try(Transaction tx = env.createWriteTransaction()) {
//                for (int j = 0; j < 10000; j++) coldb.put(tx, key(i, j), bytes("--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------"));
//                tx.commit();
//            }
//        }

// -----------------------------------------------------------------------------------------------------------------------

//        try(Transaction tx = env.createReadTransaction()) {
//            EntryIterator ei = tableaccessdb.iterate(tx);
//            while (ei.hasNext()) {
//                Entry e = ei.next();
//                System.err.println("tableaccessdb: " + Hex.encodeHexString(e.getKey()) + ":" + Hex.encodeHexString(e.getValue()));
//            }
//        }



//        try(Transaction tx = env.createReadTransaction()) {
//            EntryIterator ei = textdatadb.iterate(tx);
//            while (ei.hasNext()) {
//                Entry e = ei.next();
//                System.err.println("textdatadb: " + Hex.encodeHexString(e.getKey()) + ":" + string(e.getValue()));
//            }
//        }
//
//
//        try(Transaction tx = env.createReadTransaction()) {
//            EntryIterator ei = coldb.iterate(tx);
//            while (ei.hasNext()) {
//                Entry e = ei.next();
//                System.err.println("coldb: " + string(e.getKey()) + ":" + Hex.encodeHexString(e.getValue()));
//            }
//        }
//
//        LmdbDataManager.removeDocument("c1/d2");
//
//        Thread.sleep(1000*60*2);
//
//        try(Transaction tx = env.createReadTransaction()) {
//            EntryIterator ei = textdatadb.iterate(tx);
//            while (ei.hasNext()) {
//                Entry e = ei.next();
//                System.err.println("textdatadb: " + Hex.encodeHexString(e.getKey()) + ":" + string(e.getValue()));
//            }
//        }
//
//        try(Transaction tx = env.createReadTransaction()) {
//            EntryIterator ei = coldb.iterate(tx);
//            while (ei.hasNext()) {
//                Entry e = ei.next();
//                System.err.println("coldb: " + string(e.getKey()) + ":" + Hex.encodeHexString(e.getValue()));
//            }
//        }


//
//        try(Transaction tx = env.createReadTransaction()) {
//            EntryIterator ei = attributevaldb.iterate(tx);
//            while (ei.hasNext()) {
//                Entry e = ei.next();
//                System.err.println("attributevaldb: " + Hex.encodeHexString(e.getKey()) + ":" + string(e.getValue()));
//            }
//        }
//
//        try(Transaction tx = env.createReadTransaction()) {
//            EntryIterator ei = structdb.iterate(tx);
//            while (ei.hasNext()) {
//                Entry e = ei.next();
//                System.err.println("structdb: " + Hex.encodeHexString(e.getKey()) + ":" + string(e.getValue()));
//            }
//        }


//        XQuery.query("/root/empty",CONTENT,System.out);

//        try(LmdbQueryContext qctx = new LmdbQueryContext()) {
//            qctx.parse("collection('c1')/PLAY/TITLE");
//            qctx.compile();
//            XQuery.query(qctx, System.out, null, "true");
//        }
//
//        System.out.println("\n");
//
//        try(LmdbQueryContext qctx = new LmdbQueryContext()) {
//            qctx.parse("doc('c4/d0')//empty");
//            qctx.compile();
//            XQuery.query(qctx, System.out, null, "true");
//        }

//        try(Transaction tx = env.createReadTransaction(); LmdbQueryContext qctx = new LmdbQueryContext(tx)) {
//            Data data = openDocument("c4/d3", new MainOptions(),tx);
//            qctx.context(new DBNode(data));
//            qctx.parse("/site/regions/africa");
//            qctx.compile();
////            XQuery.query(qctx, new FileOutputStream(File.createTempFile("xxx.", ".yyy", null)), null, "true");
//            XQuery.query(qctx, System.out, null, "true");
//        }


//        try(LmdbQueryContext qctx = new LmdbQueryContext()) {
//            qctx.parse("doc('c4/d2')//TITLE[not(contains(./text(),'SCENE')) and not(contains(./text(),'ACT')) and not(contains(./text(),'Dramatis Personae'))]");
//            qctx.compile();
//            XQuery.query(qctx, System.out, null, "true");
//        }

//        try(LmdbQueryContext qctx = new LmdbQueryContext()) {
//            qctx.parse("replace value of node doc('c4/d0')//not_empty with 'HELLO'");
//            qctx.compile();
//            XQuery.query(qctx, System.out, null, true);
//        }

//        try(Transaction tx = env.createReadTransaction()) {
//            EntryIterator ei = tableaccessdb.iterate(tx);
//            while (ei.hasNext()) {
//                Entry e = ei.next();
//                System.err.println("tableaccessdb: " + Hex.encodeHexString(e.getKey()) + ":" + Hex.encodeHexString(e.getValue()));
//            }
//        }




//        try(Transaction tx = env.createReadTransaction()) {
//            EntryIterator ei = textdatadb.iterate(tx);
//            while (ei.hasNext()) {
//                Entry e = ei.next();
//                System.err.println("textdatadb: " + Hex.encodeHexString(e.getKey()) + ":" + string(e.getValue()));
//            }
//        }
//
//        try(Transaction tx = env.createReadTransaction()) {
//            EntryIterator ei = attributevaldb.iterate(tx);
//            while (ei.hasNext()) {
//                Entry e = ei.next();
//                System.err.println("attributevaldb: " + Hex.encodeHexString(e.getKey()) + ":" + string(e.getValue()));
//            }
//        }
//
//
//        try(LmdbQueryContext qctx = new LmdbQueryContext()) {
//            qctx.parse("doc('c4/d0')");
//            qctx.compile();
//            XQuery.query(qctx, System.out, null, true);
//        }

// !!!!!!!!!!!!!!!!!!!!!
//        try(Transaction tx = env.createReadTransaction()) {
//            EntryIterator ei = tableaccessdb.iterate(tx);
//            while (ei.hasNext()) {
//                Entry e = ei.next();
//
//                int c = 0;
//                byte[] v = e.getValue();
//                for(int i = 0; i+16 < v.length; i+=16) {
//                    System.err.println(c++ + ": " + Hex.encodeHexString(Arrays.copyOfRange(v,i,i+16)));
//                }
//
//                //System.err.println("tableaccessdb: " + Hex.encodeHexString(e.getKey()) + ":" + Hex.encodeHexString(e.getValue()));
//            }
//        }
//

//        try(Transaction tx = env.createReadTransaction(); EntryIterator ei = textdatadb.iterate(tx)) {
//            while (ei.hasNext()) {
//                Entry e = ei.next();
//                System.err.println("textdatadb: " + Hex.encodeHexString(e.getKey()) + ":" + string(e.getValue()));
//            }
//        }
//
//        try(Transaction tx = env.createReadTransaction(); EntryIterator ei = attributevaldb.iterate(tx)) {
//            while (ei.hasNext()) {
//                Entry e = ei.next();
//                System.err.println("attributevaldb: " + Hex.encodeHexString(e.getKey()) + ":" + string(e.getValue()));
//            }
//        }
//
//        try(LmdbQueryContext qctx = new LmdbQueryContext()) {
//            qctx.parse("insert node <new_element_a name='a'/> into doc('c4/d0')/root");
//            qctx.compile();
//            XQuery.query(qctx, System.out, null, true);
//        }
//
//
//        try(LmdbQueryContext qctx = new LmdbQueryContext()) {
//            qctx.parse("doc('c4/d0')");
//            qctx.compile();
//            XQuery.query(qctx, System.out, null, true);
//        }

//
//        try(LmdbQueryContext qctx = new LmdbQueryContext()) {
//            qctx.parse("insert node <new_element_b name='b'>b</new_element_b> into doc('c4/d0')/root");
//            qctx.compile();
//            XQuery.query(qctx, System.out, null, true);
//        }
//
//        try(LmdbQueryContext qctx = new LmdbQueryContext()) {
//            qctx.parse("insert node <new_element_b/> as first into doc('c4/d0')/root");
//            qctx.compile();
//            XQuery.query(qctx, System.out, null, true);
//        }
//
//        try(LmdbQueryContext qctx = new LmdbQueryContext()) {
//            qctx.parse("insert node <new_element_c/> as last into doc('c4/d0')/root");
//            qctx.compile();
//            XQuery.query(qctx, System.out, null, true);
//        }
//
//        try(LmdbQueryContext qctx = new LmdbQueryContext()) {
//            qctx.parse("insert node <new_element_d/> before doc('c4/d0')/root/new_element_c");
//            qctx.compile();
//            XQuery.query(qctx, System.out, null, true);
//        }
//
//        try(LmdbQueryContext qctx = new LmdbQueryContext()) {
//            qctx.parse("insert node <new_element_e/> after doc('c4/d0')/root/new_element_b");
//            qctx.compile();
//            XQuery.query(qctx, System.out, null, true);
//        }
//
//        try(LmdbQueryContext qctx = new LmdbQueryContext()) {
//            qctx.parse("delete node doc('c4/d0')/root/new_element_b");
//            qctx.compile();
//            XQuery.query(qctx, System.out, null, true);
//        }
//
//        try(LmdbQueryContext qctx = new LmdbQueryContext()) {
//            qctx.parse("replace node doc('c4/d0')/root/new_element_a with <aaa/>");
//            qctx.compile();
//            XQuery.query(qctx, System.out, null, true);
//        }
//
//        try(LmdbQueryContext qctx = new LmdbQueryContext()) {
//            qctx.parse("rename node doc('c4/d0')//empty/@att1 as 'HELLO'");
//            qctx.compile();
//            XQuery.query(qctx, System.out, null, true);
//        }

//        try(LmdbQueryContext qctx = new LmdbQueryContext(opt)) {
//            qctx.parse("doc('file://etc/books.xml')");
//            qctx.compile();
//            XQuery.query(qctx, System.out, null, true);
//        }

//        try(Transaction tx = env.createReadTransaction()) {
//            EntryIterator ei = tableaccessdb.iterate(tx);
//            while (ei.hasNext()) {
//                Entry e = ei.next();
//                System.err.println("tableaccessdb: " + Hex.encodeHexString(e.getKey()) + ":" + Hex.encodeHexString(e.getValue()));
//            }
//        }

//        try(LmdbQueryContext qctx = new LmdbQueryContext()) {
//            qctx.parse("delete node doc('c4/d0')/root/empty");
//            qctx.compile();
//            XQuery.query(qctx, System.out, null, true);
//        }
//


//        try(LmdbQueryContext qctx = new LmdbQueryContext()) {
//            qctx.parse("doc('c4/d0')");
//            qctx.compile();
//            XQuery.query(qctx, System.out, null, true);
//        }

//
//        try(Transaction tx = env.createReadTransaction(); EntryIterator ei = tableaccessdb.iterate(tx)) {
//            while (ei.hasNext()) {
//                Entry e = ei.next();
//
//                int c = 0;
//                byte[] v = e.getValue();
//                for(int i = 0; i+16 <= v.length; i+=16) {
//                    System.err.println(c++ + ": " + Hex.encodeHexString(Arrays.copyOfRange(v,i,i+16)));
//                }
//
//                //System.err.println("tableaccessdb: " + Hex.encodeHexString(e.getKey()) + ":" + Hex.encodeHexString(e.getValue()));
//            }
//        }
//
//        try(Transaction tx = env.createReadTransaction(); EntryIterator ei = textdatadb.iterate(tx)) {
//            while (ei.hasNext()) {
//                Entry e = ei.next();
//                System.err.println("textdatadb: " + Hex.encodeHexString(e.getKey()) + ":" + string(e.getValue()));
//            }
//        }
//
//        try(Transaction tx = env.createReadTransaction(); EntryIterator ei = attributevaldb.iterate(tx)) {
//            while (ei.hasNext()) {
//                Entry e = ei.next();
//                System.err.println("attributevaldb: " + Hex.encodeHexString(e.getKey()) + ":" + string(e.getValue()));
//            }
//        }
//
//        try(Transaction tx = env.createReadTransaction()) {
//            EntryIterator ei = structdb.iterate(tx);
//            while (ei.hasNext()) {
//                Entry e = ei.next();
//                System.err.println("structdb: " + Hex.encodeHexString(e.getKey()) + ":" + string(e.getValue()));
//            }
//        }
//
//
//        try(Transaction tx = env.createReadTransaction()) {
//            EntryIterator ei = textdatadb.iterate(tx);
//            while (ei.hasNext()) {
//                Entry e = ei.next();
//                System.err.println("textdatadb: " + Hex.encodeHexString(e.getKey()) + ":" + string(e.getValue()));
//            }
//        }
////
//        try(Transaction tx = env.createReadTransaction()) {
//            EntryIterator ei = attributevaldb.iterate(tx);
//            while (ei.hasNext()) {
//                Entry e = ei.next();
//                System.err.println("attributevaldb: " + Hex.encodeHexString(e.getKey()) + ":" + string(e.getValue()));
//            }
//        }


//        try(Transaction tx = env.createReadTransaction()) {
//            EntryIterator ei = tableaccessdb.iterate(tx);
//            while (ei.hasNext()) {
//                Entry e = ei.next();
//
//                int c = 0;
//                byte[] v = e.getValue();
//                for(int i = 0; i+16 <= v.length; i+=16) {
//                    System.err.println(c++ + ": " + Hex.encodeHexString(Arrays.copyOfRange(v,i,i+16)));
//                }
//
//                //System.err.println("tableaccessdb: " + Hex.encodeHexString(e.getKey()) + ":" + Hex.encodeHexString(e.getValue()));
//            }
//        }
//
//
//
//        System.out.println("\n--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
//
//        try(LmdbQueryContext qctx = new LmdbQueryContext()) {
//            qctx.parse("doc('c4/d5')//LINE");
//            qctx.compile();
//            XQuery.query(qctx, System.out, null, true);
//        }



//        try(LmdbQueryContext qctx = new LmdbQueryContext()) {
//            qctx.parse("doc('c4/d2')//LINE");
//            qctx.compile();
//            XQuery.query(qctx, System.out, null, true);
//        }


//        try(LmdbQueryContext qctx = new LmdbQueryContext()) {
//            qctx.parse("doc('c4/d1')");
//            qctx.compile();
//            XQuery.query(qctx, System.out, null, "true");
//        }

//        System.out.println("         ctx start: " + new Date());
//
//        try(LmdbQueryContext qctx = new LmdbQueryContext()) {
//            qctx.parse("count(doc('c4/d3')//node())");
//            qctx.compile();
//            XQuery.query(qctx, System.out, null, true);
////            XQuery.query(qctx, new OutputStream() {
////                boolean written = false;
////                @Override
////                public void write(int b) throws IOException {
////                    if(!written) {
////                        System.out.println(" result dump start: " + new Date());
////                        written = true;
////                    }
////
////                }}, null, true);
//        }
//        System.out.println("result dump finish: " + new Date());


//        try(LmdbQueryContext qctx = new LmdbQueryContext()) {
//            qctx.parse("count(doc('c4/d3')//node())");
//            qctx.compile();
//            XQuery.query(qctx, System.out, null, "true");
//        }

//
//        System.out.println("         ctx start: " + new Date());
//
//        try(LmdbQueryContext qctx = new LmdbQueryContext()) {
//            qctx.parse("doc('c4/d3')//Abstract");
//            qctx.compile();
////            XQuery.query(qctx, System.out, null, "true");
//            XQuery.query(qctx, new OutputStream() {
//                boolean written = false;
//                @Override
//                public void write(int b) throws IOException {
//                    if(!written) {
//                        System.out.println(" result dump start: " + new Date());
//                        written = true;
//                    }
//
//                }}, null, true);
//
//            System.out.println("result dump finish: " + new Date());
//            //System.out.println("         ctx start: " + new Date());
//
////            qctx.parse("doc('c4/d3')//Abstract");
////            qctx.compile();
////            XQuery.query(qctx, System.out, null, "true");
//            XQuery.query(qctx, new OutputStream() {
//                boolean written = false;
//                @Override
//                public void write(int b) throws IOException {
//                    if(!written) {
//                        System.out.println(" result dump start: " + new Date());
//                        written = true;
//                    }
//
//                }}, null, true);
//        }
//        System.out.println("result dump finish: " + new Date());
//
//
//        FileOutputStream fos = new FileOutputStream(File.createTempFile("xxx.", ".yyy", null));
//
//        try(LmdbQueryContext qctx = new LmdbQueryContext()) {
//            qctx.parse("doc('c4/d5')//LINE");
//            qctx.compile();
//            XQuery.query(qctx, fos, null, true);
//        }
//
//        fos.close();


//        PrintWriter p = new PrintWriter(new FileOutputStream(File.createTempFile("xxx.", ".yyy", null)));
//
//        try(Transaction tx = env.createReadTransaction()) {
//            EntryIterator ei = tableaccessdb.iterate(tx);
//            while (ei.hasNext()) {
//                Entry e = ei.next();
//                p.println("tableaccessdb: " + Hex.encodeHexString(e.getKey()) + ":" + Hex.encodeHexString(e.getValue()));
//            }
//        }
//
//        try(Transaction tx = env.createReadTransaction()) {
//            EntryIterator ei = textdatadb.iterate(tx);
//            while (ei.hasNext()) {
//                Entry e = ei.next();
//                p.println("textdatadb: " + Hex.encodeHexString(e.getKey()) + ":" + string(e.getValue()));
//            }
//        }
//
//        try(Transaction tx = env.createReadTransaction()) {
//            EntryIterator ei = attributevaldb.iterate(tx);
//            while (ei.hasNext()) {
//                Entry e = ei.next();
//                p.println("attributevaldb: " + Hex.encodeHexString(e.getKey()) + ":" + string(e.getValue()));
//            }
//        }
//
//        try(Transaction tx = env.createReadTransaction()) {
//            EntryIterator ei = coldb.iterate(tx);
//            while (ei.hasNext()) {
//                Entry e = ei.next();
//                p.println("coldb: " + Hex.encodeHexString(e.getKey()) + ":" + string(e.getValue()));
//            }
//        }
//
//


//        p.close();

//        Thread.sleep(1000);

        LmdbDataManager.stop();
    }


}

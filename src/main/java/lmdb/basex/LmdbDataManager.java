package lmdb.basex;

import org.apache.log4j.Logger;
import org.fusesource.lmdbjni.Database;
import org.fusesource.lmdbjni.Entry;
import org.fusesource.lmdbjni.EntryIterator;
import org.fusesource.lmdbjni.Env;
import org.fusesource.lmdbjni.Transaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;

import static org.fusesource.lmdbjni.Constants.FIXEDMAP;
import static org.fusesource.lmdbjni.Constants.bytes;
import static lmdb.Constants.string;

public class LmdbDataManager {
// TODO: basex-lmdb: add collections + documents cleaner based on removal flag
// TODO: basex-lmdb: make sure interrupted removeCollection operation is completed by
// TODO: basex-lmdb: checking the markings on all its documents (removeAllDocuments) on startup

    private static final Logger logger = Logger.getLogger(LmdbDataManager.class);

    private static Env env = null;
    private static Database coldb;
    private static Database pathsdb;
    private static Database namespacedb;
    private static Database elementdb;
    private static Database attributedb;
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

    private static final byte[] COLLECTION_LIST_KEY = new byte[]{0};

    public static void config(String home) {
        if(env != null) return;
        env = new Env();
        env.setMapSize(1024 * 1024 * 1024);
        env.setMaxDbs(15);
        env.open(home, FIXEDMAP);
    }

    public static void start() {
        coldb = env.openDatabase("collections");
        pathsdb = env.openDatabase("paths");
        namespacedb = env.openDatabase("namespaces");
        elementdb = env.openDatabase("element_names");
        attributedb = env.openDatabase("attribute_names");
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
        logger.info("start");
    }

    public static void stop() {
        coldb.close();
        pathsdb.close();
        namespacedb.close();
        elementdb.close();
        attributedb.close();
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
                if(collection.contains(name)) throw new IOException("collection " + name + " exists");
                collection.add(name);
                coldb.put(tx, COLLECTION_LIST_KEY, bytes(collection.toString()));
            } else {
                coldb.put(tx,COLLECTION_LIST_KEY, bytes("["+name+"]"));
            }
            tx.commit();
        }
    }

    public static List<String> listCollections() throws IOException {
        byte[] cl = coldb.get(COLLECTION_LIST_KEY);
        if (cl == null) return new ArrayList<String>();
        String[] clist = string(cl,1,cl.length-2).split(", ");
        ArrayList<String> collection = new ArrayList<String>(clist.length);
        for(String c: clist) if(!c.endsWith("/r")) collection.add(c);
        return collection;
    }

    public static synchronized void removeCollection(final String name) throws IOException {
        byte[] cl = coldb.get(COLLECTION_LIST_KEY);
        if (cl == null) return;
        String collections =string(cl,1,cl.length-2);
        HashSet<String> collection = new HashSet<String>(Arrays.asList(collections.split(", ")));
        if(collection.remove(name)) {
            collection.add(name+"/r");
            coldb.put(COLLECTION_LIST_KEY, bytes(collection.toString()));
            removeAllDocuments(name);
        }
    }

    private static synchronized void removeAllDocuments(String collection) {
        try(Transaction tx = env.createWriteTransaction()) {
            EntryIterator ei = coldb.seek(tx, bytes(collection));
            while (ei.hasNext()) {
                Entry e = ei.next();
                if(coldb.delete(tx, e.getKey())) coldb.put(tx, bytes(string(e.getKey())+"/r"), e.getValue());
            }
            tx.commit();
        }
    }

    public static void main(String[] arg) throws Exception {
        LmdbDataManager.config("/home/mscastro/dev/basex-lmdb/db");
        LmdbDataManager.start();
        LmdbDataManager.createCollection("c1");
        LmdbDataManager.createCollection("c2");
        LmdbDataManager.createCollection("c3");
        LmdbDataManager.removeCollection("c1");
        LmdbDataManager.createCollection("c1");
        LmdbDataManager.removeCollection("c1");
        System.out.println(LmdbDataManager.listCollections());
        LmdbDataManager.stop();
    }
}

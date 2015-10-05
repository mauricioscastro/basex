package lmdb.basex;

import lmdb.util.XQuery;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.basex.query.QueryException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LmdbDataManagerTest {

    static {
        System.setProperty("log4j.defaultInitOverride", "true");
        LogManager.resetConfiguration();
        LogManager.getRootLogger().removeAllAppenders();
        LogManager.getRootLogger().setLevel(Level.toLevel("off"));
    }

    private static String HOME = "./db";
    private static String TEST_COLLECTION = "etc";
    private static String XML_DIR = HOME + "/xml/etc/";

    private static void clean() throws IOException {
        FileUtils.deleteQuietly(new File(LmdbDataManager.home() + "/data.mdb"));
        FileUtils.deleteQuietly(new File(LmdbDataManager.home() + "/lock.mdb"));
    }

    @BeforeClass
    public static void oneTimeSetUp() throws Exception {
        clean();
        LmdbDataManager.config(HOME);
        LmdbDataManager.start();
    }

    @AfterClass
    public static void oneTimeTearDown() throws IOException {
        LmdbDataManager.stop();
        clean();
    }

    @Before
    public void setUp() throws IOException {
        LmdbDataManager.removeCollection(TEST_COLLECTION);
    }

    @After
    public void tearDown() throws IOException {
    }

    @Test
    public void createCollectionTest() throws IOException {
        LmdbDataManager.createCollection(TEST_COLLECTION);
        assertTrue(LmdbDataManager.listCollections().contains(TEST_COLLECTION));
    }

    @Test
    public void removeCollectionTest() throws IOException {
        createCollectionTest();
        assertTrue(LmdbDataManager.listCollections().contains(TEST_COLLECTION));
    }

    @Test
    public void createDocumentTest() throws IOException {
        LmdbDataManager.createCollection(TEST_COLLECTION);
        LmdbDataManager.createDocument(TEST_COLLECTION + "/auction", new FileInputStream(XML_DIR + "auction.xml"));
        LmdbDataManager.createDocument(TEST_COLLECTION + "/books", new FileInputStream(XML_DIR + "books.xml"));
        List d = LmdbDataManager.listDocuments(TEST_COLLECTION);
        assertTrue(d.contains("auction") && d.contains("books"));
    }

    @Test
    public void removeDocumentTest() throws IOException, QueryException {
            LmdbDataManager.createCollection(TEST_COLLECTION);
            LmdbDataManager.createDocument(TEST_COLLECTION + "/books", new FileInputStream(XML_DIR + "books.xml"));
            XQuery.getString("doc('" + TEST_COLLECTION + "/books" + "')");
            LmdbDataManager.removeDocument(TEST_COLLECTION + "/books");
            assertFalse(LmdbDataManager.listDocuments(TEST_COLLECTION).contains("books"));
    }

//    @Test
//    public void indexUsageTest() throws IOException, QueryException {
//        LmdbDataManager.createCollection(TEST_COLLECTION);
//        LmdbDataManager.createDocument(TEST_COLLECTION + "/factbook", new FileInputStream(XML_DIR + "factbook.xml"));
//        String result = XQuery.getString("doc('" + TEST_COLLECTION + "/factbook')//lake[@id='f0_39401']");
//        //System.out.println(result);
//        LmdbDataManager.removeDocument(TEST_COLLECTION + "/factbook");
//        LmdbDataManager.removeCollection(TEST_COLLECTION);
//        assertFalse(result.isEmpty());
//    }
}

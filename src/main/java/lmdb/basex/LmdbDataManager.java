package lmdb.basex;

import org.apache.log4j.Logger;
import org.fusesource.lmdbjni.Database;
import org.fusesource.lmdbjni.Env;

import static org.fusesource.lmdbjni.Constants.FIXEDMAP;

public class LmdbDataManager {

    private static final Logger logger = Logger.getLogger(LmdbDataManager.class);

    private static Env env = null;
    private static Database admindb;
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

    public static void config(String home) {
        if(env != null) return;
        env = new Env();
        env.setMapSize(1024 * 1024 * 1024);
        env.setMaxDbs(15);
        env.open(home, FIXEDMAP);
        admindb = env.openDatabase("administration");
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
        admindb.close();
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
}

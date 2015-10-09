package lmdb.db;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lmdb.basex.LmdbQueryContext;
import org.apache.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;

public class JdbcDataManager {

    private static final Logger logger = Logger.getLogger(JdbcDataManager.class);
    public static final ConcurrentHashMap<String,HikariDataSource> datasource = new ConcurrentHashMap<String,HikariDataSource>();

    protected JdbcDataManager() {}

    public static synchronized void config(String config) {
        try {
            if (Boolean.parseBoolean(LmdbQueryContext.queryString("empty(//datasources/datasource)", config))) return;
            for (String id : LmdbQueryContext.queryString("for $d in //datasources/datasource return string($d/@id)", config).split("\\s")) {
                HikariConfig hcfg = new HikariConfig();

                hcfg.setJdbcUrl(LmdbQueryContext.queryString("//datasources/datasource[@id='" + id + "']/jdbcburl/text()", config));
                hcfg.setUsername(LmdbQueryContext.queryString("//datasources/datasource[@id='" + id + "']/username/text()", config));
                hcfg.setPassword(LmdbQueryContext.queryString("//datasources/datasource[@id='" + id + "']/password/text()", config));

                for (String name : LmdbQueryContext.queryString("for $p in //datasources/datasource[@id='" + id + "']/property return string($p/@name)", config).split("\\s")) {
                    if (name.trim().isEmpty()) continue;
                    hcfg.addDataSourceProperty(name, LmdbQueryContext.queryString("//datasources/datasource[@id='" + id + "']/property[@name='" + name + "']/text()", config));
                }
                datasource.put(id, new HikariDataSource(hcfg));
            }
        } catch(Exception e) {
            logger.warn(e.getMessage());
            if (logger.isDebugEnabled()) logger.debug("", e);
        }
    }

    public static synchronized void stop() {
        for(HikariDataSource ds: datasource.values()) try {
            ds.close();
        } catch(Exception i) {}
    }
}

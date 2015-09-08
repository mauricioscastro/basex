package lmdb.server;

import lmdb.basex.LmdbDataManager;
import lmdb.db.JdbcDataManager;
import lmdb.handler.XQueryHandler;
import lmdb.util.XQuery;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import org.basex.query.QueryException;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.util.thread.ShutdownThread;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.StringReader;
import java.net.URISyntaxException;
import java.nio.file.Paths;

public class XQueryServer {

    private static String home;

    static {
        try {
            home = Paths.get(XQueryServer.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getParent().toString();
            System.setProperty("org.basex.path", home+"/db");
        } catch(URISyntaxException i) { }
        System.setProperty("log4j.defaultInitOverride", "true");
        LogManager.resetConfiguration();
        LogManager.getRootLogger().removeAllAppenders();
        LogManager.getRootLogger().setLevel(Level.toLevel("off"));
    }

    private static final Logger logger = Logger.getLogger(XQueryServer.class);
    private static final Server server = new Server(new QueuedThreadPool());
    public static QueuedThreadPool threadPool = (QueuedThreadPool)server.getThreadPool();
    private static String config;
    private static XQueryServer xqserver = null;

    public static void main(String[] arg) {
        try {
            if(arg.length > 0) {
                home = arg[0];
                System.setProperty("org.basex.path", home+"/db");
            }
            xqserver = new XQueryServer(new File(home+"/etc/config.xml"));
            xqserver.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public XQueryServer(File cfg) throws Exception {
        config = IOUtils.toString(new FileInputStream(cfg));
        config();
    }

    protected void config() throws Exception {

        ShutdownThread.register(new AbstractLifeCycle() {
            @Override
            public void setStopTimeout(long stopTimeout) {
                super.setStopTimeout(1000 * 5);
            }
        });

        configLogging();
        logger.info("start");
        logger.debug("home=" + home);

        LmdbDataManager.config(System.getProperty("org.basex.path",home+"/db"));

        threadPool.setMaxThreads(1000);
        threadPool.setMinThreads(250);

        JdbcDataManager.config(config);
        httpServerConfig();


    }

    private void httpServerConfig() {

        server.setAttribute("org.eclipse.jetty.server.Request.maxFormContentSize", getUploadLimit() * 1024 * 1024 * 1024);

        HttpConfiguration httpConfig = new HttpConfiguration();
        httpConfig.setSecureScheme("https");
        httpConfig.setSecurePort(getHttpsPort());
        httpConfig.setOutputBufferSize(32768);

        ServerConnector connector = new ServerConnector(server,new HttpConnectionFactory(httpConfig));
        connector.setPort(getHttpPort());
        connector.setIdleTimeout(30000);

        HttpConfiguration httpsConfig = new HttpConfiguration(httpConfig);
        httpsConfig.addCustomizer(new SecureRequestCustomizer());

        SslContextFactory sslContextFactory = new SslContextFactory();

        sslContextFactory.setWantClientAuth(true);
        sslContextFactory.setNeedClientAuth(false);

        sslContextFactory.setKeyStorePath(home + "/etc/keystore");
        String pwd = "bsocial-xqbuilder";
        sslContextFactory.setKeyStorePassword(pwd);
        sslContextFactory.setKeyManagerPassword(pwd);

        ServerConnector sslConnector = new ServerConnector(server, new SslConnectionFactory(sslContextFactory,"http/1.1"), new HttpConnectionFactory(httpsConfig));
        sslConnector.setPort(getHttpsPort());
        sslConnector.setIdleTimeout(500000);

        server.setConnectors(new Connector[]{connector, sslConnector});

        ResourceHandler resourceHandler = new ResourceHandler();
        resourceHandler.setResourceBase(home+"/www");

        HandlerList hlist = new HandlerList();

        //IPAccessHandler iph = configIPaccess();
        //if (iph != null) hlist.addHandler(iph);

        //WebAppContext webAppContext = new WebAppContext();
        //webAppContext.setContextPath("/");
        //webAppContext.setDescriptor(cfgdir + "/web.xml");
        //webAppContext.setResourceBase(cfgdir);
        //webAppContext.setParentLoaderPriority(true);

        //hlist.addHandler(new ClientCertificateAuthenticationHandler());
        hlist.addHandler(resourceHandler);
        hlist.addHandler(new XQueryHandler());
        server.setHandler(hlist);
    }

    public void start() throws Exception {
        if (server.isStarted()) return;
        Runtime.getRuntime().addShutdownHook(new ShutdownHook());
        LmdbDataManager.start();
        server.start();
        server.join();
        while(!server.isStopped());
    }

    public void stop() throws Exception {
        JdbcDataManager.stop();
        LmdbDataManager.stop();
        server.stop();
        threadPool.stop();
        logger.info("stop");
    }

//    private IPAccessHandler configIPaccess() {
//        String[] white = ConfigHelper.getConfigNodeValues(config, xpathWhiteList);
//        String[] black = ConfigHelper.getConfigNodeValues(config, xpathBlackList);
//
//        if (white.length == 0 && black.length == 0) return null;
//
//        ArrayList<String> wl = new ArrayList(Arrays.asList(white));
//        ArrayList<String> bl = new ArrayList(Arrays.asList(black));
//
//        for (ListIterator<String> li = wl.listIterator(); li.hasNext(); ) if (li.next().isEmpty()) li.remove();
//        for (ListIterator<String> li = bl.listIterator(); li.hasNext(); ) if (li.next().isEmpty()) li.remove();
//
//        if (wl.isEmpty() && bl.isEmpty()) return null;
//
//        IPAccessHandler iph = new IPAccessHandler();
//
//        if (!wl.isEmpty()) iph.setWhite(wl.toArray(new String[wl.size()]));
//        if (!bl.isEmpty()) iph.setBlack(bl.toArray(new String[bl.size()]));
//
//        logger.info(iph.dump());
//
//        return iph;
//    }

    private int getHttpPort() {
        return Integer.parseInt(getConfig("//http/port/text()"));
    }

    private long getUploadLimit() {
        try {
            return Long.parseLong(getConfig("//http/uploadLimit/text()"));
        } catch(Exception i) {
            return 100;
        }
    }

    private int getHttpsPort() {
        return Integer.parseInt(getConfig("//http/sslport/text()"));
    }

    private void configLogging() throws Exception {
        try {
            DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            InputSource is = new InputSource(new StringReader(getConfig("declare namespace log4j = 'http://jakarta.apache.org/log4j/'; //log4j:configuration")));
            DOMConfigurator.configure((Element) (db.parse(is)).getDocumentElement());
        } catch(Exception e) {
            throw new Exception("logging configuration error:" + e.getMessage());
        }
    }

    private class ShutdownHook extends Thread {
        public void run() {
            try {
                xqserver.stop();
            } catch (Exception e) {
                logger.warn(e.getMessage());
                if (logger.isDebugEnabled()) logger.debug("", e);
            }
        }
    }

    private String getConfig(String query) {
        try {
            return XQuery.getString(query, config);
        } catch (QueryException e) {
            logger.warn(e.getMessage());
            if (logger.isDebugEnabled()) logger.debug("", e);
            return null;
        }
    }
}

package lmdb.server;


import org.eclipse.jetty.util.ssl.SslContextFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;
import java.security.KeyStore;
import java.util.Collection;

public class XQuerySslContextFactory extends SslContextFactory {
    public KeyManager[] getKeyManagers() throws Exception {
        return super.getKeyManagers(null);
    }
    protected TrustManager[] getTrustManagers(KeyStore trustStore, Collection crls) throws Exception {
        return TRUST_ALL_CERTS;
    }
}

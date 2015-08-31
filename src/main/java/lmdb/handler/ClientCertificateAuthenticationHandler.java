package lmdb.handler;

import lmdb.util.MD5;
import org.apache.log4j.Logger;
import org.bouncycastle.asn1.x509.Certificate;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;

public class ClientCertificateAuthenticationHandler extends AbstractHandler {

    private static final Logger logger = Logger.getLogger(ClientCertificateAuthenticationHandler.class);

    public ClientCertificateAuthenticationHandler() {
        if(logger.isDebugEnabled()) logger.debug("ClientCertificateAuthenticationHandler init");
    }

    public void handle(String target, Request basereq, HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
        basereq.extractParameters();
        X509Certificate[] certs = (X509Certificate[])req.getAttribute("javax.servlet.request.X509Certificate");
        if(certs != null) {
            for(int n = 0; n < certs.length; n++) {
                String cert = certs[n].toString();
                logger.info("client certificate " + MD5.get(cert) + " = " + cert);
                try {
                    logger.info("client certificate subject=" + Certificate.getInstance(certs[n].getEncoded()).getSubject().toString());
                }  catch(CertificateEncodingException cpe) {
                    logger.warn(cpe.getMessage());
                    if(logger.isDebugEnabled()) logger.debug("", cpe);
                }
            }
        }
        basereq.setHandled(false);
    }
}

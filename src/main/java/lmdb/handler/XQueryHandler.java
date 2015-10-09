package lmdb.handler;

import lmdb.basex.LmdbDataManager;
import lmdb.basex.LmdbQueryContext;
import org.apache.log4j.Logger;
import org.basex.core.MainOptions;
import org.basex.query.QueryException;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.util.MultiMap;
import org.fusesource.lmdbjni.LMDBException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

@SuppressWarnings("unchecked")
public class XQueryHandler extends AbstractHandler {

    private static final Logger logger = Logger.getLogger(XQueryHandler.class);
    private MainOptions options = new MainOptions();

    public XQueryHandler() {
        options.set(MainOptions.XMLPATH, LmdbDataManager.home() + "/xml");
        if(logger.isDebugEnabled()) logger.debug("XQueryHandler init");
    }

    public void handle(String target, Request basereq, HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {

        //LmdbQueryContext ctx = null;
        try {
            //
            // DELETE
            //
            if (basereq.getMethod().equals("DELETE")) {
                resp.setContentType("text/plain");
                try {
                    String path = req.getPathInfo().substring(1).trim();
                    if (path.indexOf('/') > -1) {
                        logger.info("remove document " + path);
                        LmdbDataManager.removeDocument(path);
                    } else {
                        logger.info("remove collection " + path);
                        LmdbDataManager.removeCollection(path);
                    }
                } catch (Exception e) {
                    logger.warn(e.getMessage());
                    if (logger.isDebugEnabled()) logger.debug("", e);
                }
                resp.setStatus(HttpServletResponse.SC_OK);

            }
            //
            // PUT
            //
            if (basereq.getMethod().equals("PUT")) {
                resp.setContentType("text/plain");
                try {
                    String path = req.getPathInfo().substring(1).trim();
                    if (path.indexOf('/') > -1) {
                        String[] p = path.split("/");
                        if (p.length > 2) {
                            StringBuilder sb = new StringBuilder(p[2]);
                            for (int i = 3; i < p.length; i++) sb.append('/').append(p[i]);
                            String xquery = sb.toString();
                            logger.info("create document " + p[0] + "/" + p[1] + " as result of the xquery: " + xquery);
                            File tmp = new File(System.getProperty("java.io.tmpdir", "/tmp"), p[0] + "." + p[1] + ".xml");
                            tmp.deleteOnExit();
                            FileOutputStream tmpos = new FileOutputStream(tmp);
                            try(LmdbQueryContext ctx = new LmdbQueryContext(xquery)) {
                                if (ctx.updating) throw new HttpException(405, "xquery is updating. use post instead.");
                                ctx.run(tmpos);
                                LmdbDataManager.createDocument(p[0] + "/" + p[1], new FileInputStream(tmp));
                            } finally {
                                tmpos.close();
                                tmp.delete();
                            }
                        } else {
                            logger.info("create document " + path);
                            LmdbDataManager.createDocument(path, req.getInputStream());
                        }
                    } else {
                        logger.info("create collection " + path);
                        LmdbDataManager.createCollection(path);
                    }
                    resp.setStatus(HttpServletResponse.SC_OK);
                } catch (LMDBException lmdbe) {
                    logger.warn(lmdbe.getMessage());
                    if (logger.isDebugEnabled()) logger.debug("", lmdbe);
                    resp.setContentType("text/plain");
                    resp.setStatus(500);
                } catch (QueryException qe) {
                    logger.warn(qe.getMessage());
                    if (logger.isDebugEnabled()) logger.debug("", qe);
                    resp.setStatus(500);
                    resp.getWriter().print(qe.getMessage() + qe.info() == null ? "" : ": line: " + qe.info().line() + " column: " + qe.info().column());
                } catch (HttpException httpe){
                    logger.warn(httpe.toString());
                    if (logger.isDebugEnabled()) logger.debug("", httpe);
                    resp.setContentType("text/plain");
                    resp.setStatus(httpe.getStatus());
                    resp.getWriter().print(httpe.getReason());
                }
            }
            //
            // GET
            //
            if (basereq.getMethod().equals("GET")) {
                basereq.extractParameters();
                MultiMap param = basereq.getQueryParameters();
                String contentType = param.getString("content-type");
                String indentContent = param.getString("indent-content");
                if (contentType != null) {
                    param.remove("content-type");
                } else {
                    contentType = "text/xml";
                }
                resp.setContentType(contentType);
                OutputStream os = resp.getOutputStream();
                try(LmdbQueryContext ctx = new LmdbQueryContext(req.getPathInfo().substring(1).trim(), param, options)) {
//                    ctx = XQuery.getContext(req.getPathInfo().substring(1).trim(), param);
//                    ctx.options = options;
                    if (ctx.updating) throw new HttpException(405, "xquery is updating. use post instead.");
//                    XQuery.query(ctx, os, contentType, indentContent);
                    ctx.run(os, contentType, indentContent);
                    resp.setStatus(HttpServletResponse.SC_OK);
                } catch (LMDBException lmdbe) {
                    logger.warn(lmdbe.getMessage());
                    if (logger.isDebugEnabled()) logger.debug("", lmdbe);
                    resp.setContentType("text/plain");
                    resp.setStatus(500);
                } catch (HttpException httpe) {
                    logger.warn(httpe.toString());
                    if (logger.isDebugEnabled()) logger.debug("", httpe);
                    resp.setContentType("text/plain");
                    resp.setStatus(httpe.getStatus());
                    os.write(httpe.getReason().getBytes());
                    //resp.getWriter().print(httpe.getReason());
                } catch (QueryException qe) {
                    logger.warn(qe.getMessage());
                    if (logger.isDebugEnabled()) logger.debug("", qe);
                    resp.setContentType("text/plain");
                    resp.setStatus(500);
                    os.write(qe.getMessage().getBytes()); //(qe.getMessage() + (qe.info() == null ? "" : ": line: " + qe.info().line() + " column: " + qe.info().column())).getBytes());
                    //resp.getWriter().print(qe.getMessage() + ": line: " + qe.info().line() + " column: " + qe.info().column());
                } catch (Exception e) {
                    logger.warn(e.getMessage());
                    if (logger.isDebugEnabled()) logger.debug("", e);
                    resp.setContentType("text/plain");
                    resp.setStatus(500);
                    os.write(e.getMessage().getBytes());
                    //resp.getWriter().print(e.getMessage());
                } finally {
//                try { if(ctx != null) ctx.close(); } catch (Exception i) {}
                    try {
                        os.close();
                    } catch (Exception i) {
                    }
                }
            }
            //
            // POST
            //
//            if (basereq.getMethod().equals("POST")) {
//                resp.setContentType("text/plain");
//                try {
//                    ctx = XQuery.getContext(IOUtils.toString(req.getInputStream()));
//                    ctx.options = options;
//                    if (!ctx.updating) throw new HttpException(405, "xquery is not updating. use get instead.");
//                    XQuery.query(ctx, resp.getOutputStream(), "text/plain", null);
//                    resp.setStatus(HttpServletResponse.SC_OK);
//                } catch (LMDBException lmdbe) {
//                    logger.warn(lmdbe.getMessage());
//                    if (logger.isDebugEnabled()) logger.debug("", lmdbe);
//                    resp.setContentType("text/plain");
//                    resp.setStatus(500);
//                } catch (HttpException httpe) {
//                    logger.warn(httpe.toString());
//                    if (logger.isDebugEnabled()) logger.debug("", httpe);
//                    resp.setStatus(httpe.getStatus());
//                    resp.getWriter().print(httpe.getReason());
//                } catch (QueryException qe) {
//                    logger.warn(qe.getMessage());
//                    if (logger.isDebugEnabled()) logger.debug("", qe);
//                    resp.setStatus(500);
//                    resp.getWriter().print(qe.getMessage() + qe.info() == null ? "" : ": line: " + qe.info().line() + " column: " + qe.info().column());
//                }
//            }
        } finally {
            //if(ctx != null) ctx.close();
        }
        basereq.setHandled(true);
    }

    private class HttpException extends Exception {
        private int status;
        public HttpException(int status, String msg) {
            super(msg);
            this.status = status;
        }
        int getStatus() { return status; }
        String getReason() { return super.getMessage(); }
    }
}

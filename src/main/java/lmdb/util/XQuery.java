package lmdb.util;

import org.basex.build.json.JsonSerialOptions;
import org.basex.io.IOContent;
import org.basex.io.serial.SerialMethod;
import org.basex.io.serial.Serializer;
import org.basex.io.serial.SerializerOptions;
import org.basex.query.QueryException;
import org.basex.query.iter.Iter;
import org.basex.query.value.item.Item;
import org.basex.query.value.node.DBNode;
import org.basex.query.value.type.NodeType;
import org.basex.query.value.type.SeqType;

import lmdb.basex.QueryContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

@SuppressWarnings("unchecked")
public class XQuery {

    protected XQuery() {
    }

    public static QueryContext getContext(String query, String context, Map<String,Object> var) throws QueryException {
        try {
            QueryContext ctx = new QueryContext();
            ctx.parse(query);
            if(context != null) ctx.context(new DBNode(new IOContent(context)));
            if(var != null && var.size() > 0) for(String k: var.keySet()) ctx.bind(k, var.get(k));
            ctx.compile();
            return ctx;
        } catch(IOException ioe) {
            throw new QueryException(ioe);
        }
    }

    public static QueryContext getContext(String query, Map<String,Object> var) throws QueryException {
        return getContext(query, null, var);
    }

    public static QueryContext getContext(String query) throws QueryException {
        return getContext(query, null, null);
    }

    public static void query(String query, String context, OutputStream result, Map<String,Object> var, String method) throws QueryException {
        query(getContext(query, context, var), result, method, null);
    }

    public static void query(QueryContext ctx, OutputStream result, String method, String indent) throws QueryException {
        query(ctx, result, method, Boolean.parseBoolean(indent));
    }

    public static void query(QueryContext ctx, OutputStream result, String method, boolean indent) throws QueryException {
        try {
            Serializer s = Serializer.get(result, getSerializerOptions(method, indent));
            Iter iter = ctx.iter();
            Item i = null;
            while ((i = iter.next()) != null) {
                if(i.type == NodeType.ATT || i.type == NodeType.NSP || i.type.instanceOf(SeqType.ANY_ARRAY)) {
                    result.flush();
                    result.write(i.toString().getBytes());
                } else {
                    s.serialize(i);
                }
            }
            s.close();
        } catch(IOException ioe) {
            throw new QueryException(ioe);
        } finally {
            try { ctx.close(); } catch(IOException ioe) { throw new QueryException(ioe); }
        }
    }

    public static void query(QueryContext ctx, OutputStream result) throws QueryException {
        query(ctx, result, null, null);
    }

    public static void query(String query, OutputStream result) throws QueryException {
        query(query, null, result, null, null);
    }

    public static void query(String query, String context, OutputStream result) throws QueryException {
        query(query, context, result, null, null);
    }

    public static void query(String query, String context, OutputStream result, Map<String,Object> var) throws QueryException {
        query(query, context, result, var, null);
    }

    public static void query(String query, OutputStream result, Map<String,Object> var) throws QueryException {
        query(query, null, result, var, null);
    }

    public static String getString(String query, String context, Map<String, Object> var, String method) throws QueryException {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        query(query, context, result, var, method);
        return result.toString();
    }

    public static String getString(String query) throws QueryException {
        return getString(query, null, (Map) null, null);
    }

    public static String getString(String query, String context) throws QueryException {
        return getString(query, context, (Map) null, null);
    }

    public static String getString(String query, String context, Map<String,Object> var) throws QueryException {
        return getString(query, context, var, null);
    }

    public static InputStream getStream(final String query, final String context, final Map<String, Object> var, final String method) throws QueryException {
        try {
            return new InputStream() {
                private ByteArrayOutputStream result = new ByteArrayOutputStream();
                private Serializer s = Serializer.get(result, getSerializerOptions(method));
                private QueryContext ctx = getContext(query, context, var);
                private Iter iter = ctx.iter();
                private Item i = null;
                private byte[] b = null;
                private int off = -1;
                public void close() throws IOException {
                    s.close();
                    ctx.close();
                }
                public int read() throws IOException {
                    if((b == null || off >= b.length) && !next()) return -1;
                    return (int)b[off++];
                }
                private boolean next() throws IOException {
                    try {
                        if((i = iter.next()) == null) return false;
                        result.reset();
                        if(i.type == NodeType.ATT || i.type == NodeType.NSP || i.type.instanceOf(SeqType.ANY_ARRAY)) {
                            result.flush();
                            result.write(i.toString().getBytes());
                        } else {
                            s.serialize(i);
                        }
                        b = result.toByteArray();
                        off = 0;
                        return true;
                    } catch(QueryException qe) {
                        throw new IOException(qe);
                    }
                }
            };
        } catch(IOException ioe) {
            throw new QueryException(ioe);
        }
    }

    public static InputStream getStream(final String query, final String context, final Map<String,Object> var) throws QueryException {
        return getStream(query, context, var, null);
    }

    public static InputStream getStream(final String query, final Map<String,Object> var) throws QueryException {
        return getStream(query, null, var, null);
    }

    public static InputStream getStream(final String query, final String context) throws QueryException {
        return getStream(query, context, null, null);
    }

    public static InputStream getStream(final String query) throws QueryException {
        return getStream(query, null, null, null);
    }

    private static SerializerOptions getSerializerOptions(String method, boolean indent) {
        method = method == null ? "text/xml" : method.toLowerCase();
        SerializerOptions opt = SerializerOptions.get(indent);
        opt.set(SerializerOptions.METHOD, SerialMethod.XML);
        if(method != null && !method.contains("/xml")) {
            if (method.contains("plain")) opt.set(SerializerOptions.METHOD, SerialMethod.TEXT);
            else if (method.contains("xhtml")) opt.set(SerializerOptions.METHOD, SerialMethod.XHTML);
            else if (method.contains("html")) opt.set(SerializerOptions.METHOD, SerialMethod.HTML);
            else if (method.contains("json") || method.contains("javascript")) {
                JsonSerialOptions jsopt = new JsonSerialOptions();
                jsopt.set(JsonSerialOptions.FORMAT,"jsonml");
                opt.set(SerializerOptions.JSON, jsopt);
                opt.set(SerializerOptions.METHOD, SerialMethod.JSON);
            }
            else if (method.contains("raw")) opt.set(SerializerOptions.METHOD, SerialMethod.RAW);
        }
        return opt;
    }

    private static SerializerOptions getSerializerOptions(String method) {
        return getSerializerOptions(method, false);
    }
}

package lmdb.basex;

import lmdb.db.JdbcDataManager;
import org.apache.commons.io.FilenameUtils;
import org.basex.build.MemBuilder;
import org.basex.build.xml.XMLParser;
import org.basex.core.MainOptions;
import org.basex.data.Data;
import org.basex.io.IO;
import org.basex.io.IOStream;
import org.basex.query.QueryContext;
import org.basex.query.QueryException;
import org.basex.query.value.Value;
import org.basex.query.value.item.QNm;
import org.basex.query.value.node.ANode;
import org.basex.query.value.node.DBNode;
import org.basex.query.value.node.FElem;
import org.basex.query.value.seq.Empty;
import org.basex.util.InputInfo;
import org.basex.util.QueryInput;
import org.fusesource.lmdbjni.Transaction;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.jdbc.support.rowset.SqlRowSetMetaData;

import javax.sql.DataSource;
import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

public class QueryResources extends org.basex.query.QueryResources {

    Transaction tx = null;

    QueryResources(final QueryContext qc, Transaction tx) {
        super(qc);
        this.tx = tx;
    }

    @Override
    public Value collection(final QueryInput qi, final IO baseIO, final InputInfo info) throws QueryException {
      List col = null;
      try {
          String docURI = qi.original.trim();
          if(docURI.startsWith("bxl://")) docURI = docURI.substring(6);
          col = LmdbDataManager.listDocuments(docURI, true);
      } catch (IOException e) {
          throw new QueryException(e);
      }
      docs.addAll(col);
      return docs.isEmpty() ? Empty.SEQ : new LazyDBNodeSeq(col, qc.options, tx);
    }

    @Override
    protected void close() {
        super.close();
        if(tx == null) return;
        if(!tx.isReadOnly()) tx.commit();
        tx.close();
    }

    @Override
    protected ANode resolveURI(String uri) throws IOException {

        // TODO: basex-lmdb: use http://htmlunit.sourceforge.net/gettingLatestCode.html for http and https
        // TODO: basex-lmdb: review
        if (uri.startsWith("bxl://")) {
            String docURI = uri.substring(6);
            Data d = LmdbDataManager.openDocument(docURI, qc.options, tx);
            if(d == null) throw new IOException("error opening document " + uri);
            data.add(d);
            return new DBNode(d);
        }

        if (uri.startsWith("file://")) {
            File d = new File(FilenameUtils.normalize(qc.options.get(MainOptions.XMLPATH) + "/" + uri.substring(7)));
            if (!d.exists()) throw new FileNotFoundException(uri);
            return new DBNode(MemBuilder.build(uri, new XMLParser(new IOStream(new FileInputStream(d)), qc.options)));
        }

        if (uri.startsWith("jdbc://")) {
            if(JdbcDataManager.datasource.isEmpty()) throw new IOException("JdbcDataManager must be configured for jdbc:// uri to work correctly");
            int si = uri.indexOf('/', 8);
            String dsName = uri.substring(8, si);
            String sql = uri.substring(si + 1);
            DataSource ds = JdbcDataManager.datasource.get(dsName);
            if(ds == null) throw new IOException("can't find datasource=" + dsName + " from uri: " + uri);
            JdbcTemplate jdbct = new JdbcTemplate(ds);
            SqlRowSet sqlrs = jdbct.queryForRowSet(sql);
            SqlRowSetMetaData sqlrmd = sqlrs.getMetaData();
            FElem rdbmsDoc = new FElem(new QNm("ResultSet"));
            while (sqlrs.next()) {
                FElem row = new FElem(new QNm("Row"));
                for (int i = 1; i <= sqlrmd.getColumnCount(); i++) {
                    FElem col = new FElem(new QNm(sqlrmd.getColumnLabel(i)));
                    Object obj = sqlrs.getObject(i);
                    if (obj == null) continue;
                    if (obj.getClass().getName().contains("javax.sql.rowset.serial.")) {
                        try {
                            if (obj.getClass().getName().contains("Blob"))
                                obj = new String(((SerialBlob) obj).getBytes(1L, (int) ((SerialBlob) obj).length()));
                            if (obj.getClass().getName().contains("Clob"))
                                obj = ((SerialClob) obj).getSubString(1L, (int) ((SerialClob) obj).length());
                        } catch(Exception e) {
                            continue;
                        }
                    }
                    col.add(obj.toString().getBytes());
                    row.add(col);
                }
                rdbmsDoc.add(row);
            }
            return rdbmsDoc;
        }

        return null;

    }


}

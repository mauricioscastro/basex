package lmdb.basex;

import org.basex.data.Data;
import org.basex.io.IO;
import org.basex.query.QueryContext;
import org.basex.query.QueryException;
import org.basex.query.value.Value;
import org.basex.query.value.node.ANode;
import org.basex.query.value.node.DBNode;
import org.basex.query.value.seq.Empty;
import org.basex.util.InputInfo;
import org.basex.util.QueryInput;
import org.fusesource.lmdbjni.Transaction;

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
    protected ANode resolveBasexLmdb(String uri) throws IOException {
        String docURI = uri.substring(6);
        Data d = LmdbDataManager.openDocument(docURI, qc.options, tx);
        if(d == null) throw new IOException("error opening document " + uri);
        data.add(d);
        return new DBNode(d);
    }

    @Override
    protected void close() {
        super.close();
        if(tx == null) return;
        if(!tx.isReadOnly()) tx.commit();
        tx.close();
    }
}

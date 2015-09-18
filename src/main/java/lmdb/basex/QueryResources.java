package lmdb.basex;

import org.basex.data.Data;
import org.basex.query.QueryContext;
import org.basex.query.value.node.ANode;
import org.basex.query.value.node.DBNode;
import org.fusesource.lmdbjni.Transaction;

import java.io.IOException;

public class QueryResources extends org.basex.query.QueryResources {

    Transaction tx;

    public QueryResources(final QueryContext qc, Transaction tx) {
        super(qc);
        this.tx = tx;
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
        if(!tx.isReadOnly()) tx.commit();
        tx.close();
    }
}

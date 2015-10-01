package lmdb.basex;

import org.basex.core.MainOptions;
import org.basex.query.MainModule;
import org.basex.query.QueryException;
import org.basex.query.StaticContext;
import org.fusesource.lmdbjni.Transaction;

import java.io.Closeable;

public class QueryContext extends org.basex.query.QueryContext implements Closeable {

    public QueryContext() {
        this(new MainOptions(),null);
    }

    public QueryContext(final MainOptions opt) {
        this(opt, null);
    }

    public QueryContext(Transaction tx) {
        this(new MainOptions(), tx);
    }

    public QueryContext(final MainOptions opt, Transaction tx) {
        super(opt, null);
        resources = new QueryResources(this,tx);
    }

    @Override
    public MainModule parseMain(final String query, final String path, final StaticContext sc) throws QueryException {
        super.parseMain(query, path, sc);
        createTransaction();
        return root;
    }

    @Override
    public void mainModule(final MainModule rt) {
        super.mainModule(rt);
        createTransaction();
    }

    @Override
    public void updating() {
        super.updating();
        createTransaction();
    }

    private void createTransaction() {
        QueryResources res = (QueryResources)resources;
        if(res.tx != null) {
            if(res.tx.isReadOnly() && updating) {
                res.tx.close();
                res.tx = LmdbDataManager.env.createWriteTransaction();
            }
            return;
        }
        res.tx = updating ? LmdbDataManager.env.createWriteTransaction() :  LmdbDataManager.env.createReadTransaction();
    }
}

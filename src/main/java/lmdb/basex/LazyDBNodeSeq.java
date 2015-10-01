package lmdb.basex;


import org.basex.core.MainOptions;
import org.basex.io.IOContent;
import org.basex.query.QueryException;
import org.basex.query.expr.Expr;
import org.basex.query.value.Value;
import org.basex.query.value.item.Item;
import org.basex.query.value.node.DBNode;
import org.basex.query.value.seq.Empty;
import org.basex.query.value.seq.Seq;
import org.basex.query.value.type.NodeType;
import org.basex.query.value.type.SeqType;
import org.basex.util.InputInfo;
import org.basex.util.Util;
import org.fusesource.lmdbjni.Transaction;

import java.io.IOException;
import java.util.List;

public class LazyDBNodeSeq extends Seq {

    private List<String> doc;
    private MainOptions options;
    private Transaction tx;

    public LazyDBNodeSeq(List<String> docs, MainOptions opt, Transaction tx) {
        super(docs.size(), NodeType.DOC);
        doc = docs;
        options = opt;
        this.tx = tx;
    }

    @Override
    public Item ebv(final org.basex.query.QueryContext ctx, final InputInfo ii) {
        return itemAt(0);
    }

    @Override
    public SeqType seqType() {
        return SeqType.DOC_O;
    }

    @Override
    public boolean iterable() {
        return true;
    }

    @Override
    public boolean sameAs(final Expr cmp) {
        if(!(cmp instanceof LazyDBNodeSeq)) return false;
        final LazyDBNodeSeq seq = (LazyDBNodeSeq)cmp;
        return doc.equals(seq.doc);
    }

    @Override
    public int writeTo(final Item[] arr, final int start) {
        int w = 0;
        for(String d: doc) {
            DBNode dbn = null;
            try {
                arr[start+w] = new DBNode(LmdbDataManager.openDocument(d, options, tx));
                w++;
            } catch(IOException ioe) {
                try {
                    dbn = new DBNode(new IOContent("<error>" + ioe.getMessage() + "</error>"));
                    arr[start+w] = dbn;
                    w++;
                } catch(Exception e) {}
                throw Util.notExpected(ioe);
            }
        }
        return w;
    }

    @Override
    public DBNode itemAt(final long pos) {
        try {
            return new DBNode(LmdbDataManager.openDocument(doc.get((int) pos), options, tx));
        } catch(IOException ioe) {
            try {
                return new DBNode(new IOContent("<error>" + ioe.getMessage() + "</error>"));
            } catch(Exception i) {}
            throw Util.notExpected(ioe);
        }
    }

    public Value reverse() {
        return Empty.SEQ;
    };

    @Override
    public Value materialize(InputInfo ii) throws QueryException {
        return null;
    }

    @Override
    public Value atomValue(InputInfo ii) throws QueryException {
        return null;
    }

    @Override
    public long atomSize() {
        return 0;
    }

    @Override
    public boolean homogeneous() {
        return true;
    }

    @Override
    public Value insert(long pos, Item val) {
        return null;
    }

    @Override
    public Value remove(long pos) {
        doc.remove((int)pos);
        return this;
    }
}

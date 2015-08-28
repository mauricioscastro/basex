package org.basex.query.func.fn;

import org.basex.core.locks.DBLocking;
import org.basex.query.QueryContext;
import org.basex.query.QueryException;
import org.basex.query.func.StandardFunc;
import org.basex.query.util.ASTVisitor;
import org.basex.query.value.Value;
import org.basex.query.value.item.Item;
import org.basex.query.value.item.Str;
import org.basex.query.value.item.Uri;
import org.basex.query.value.node.ANode;
import org.basex.util.QueryInput;

import static org.basex.query.QueryError.INVCOLL_X;
import static org.basex.query.QueryError.INVDOC_X;
import static org.basex.query.func.Function.COLLECTION;
import static org.basex.query.func.Function.URI_COLLECTION;
import static org.basex.util.Token.string;

/**
 * Document and collection functions.
 *
 * @author BaseX Team 2005-15, BSD License
 * @author Christian Gruen
 */
public abstract class Docs extends StandardFunc {
  /** Special lock identifier for collection available via current context; will be substituted. */
  public static final String COLL = DBLocking.PREFIX + "COLL";

  /**
   * Returns a collection.
   * @param qc query context
   * @return collection
   * @throws QueryException query exception
   */
  Value collection(final QueryContext qc) throws QueryException {
    // return default collection
    final Item it = exprs.length == 0 ? null : exprs[0].atomItem(qc, info);
    if(it == null) return qc.resources.collection(info);

    // check if reference is valid
    final byte[] in = toToken(it);
    if(!Uri.uri(in).isValid()) throw INVCOLL_X.get(info, in);
    return qc.resources.collection(new QueryInput(string(in)), sc.baseIO(), info);
  }

  /**
   * Performs the doc function.
   * @param qc query context
   * @return result
   * @throws QueryException query exception
   */
  ANode doc(final QueryContext qc) throws QueryException {
    final Item it = exprs[0].item(qc, info);
    if(it == null) return null;
    final byte[] in = toToken(it);
    if(!Uri.uri(in).isValid()) throw INVDOC_X.get(info, in);
    //return qc.resources.doc(new QueryInput(string(in)), sc.baseIO(), info);
    return qc.resources.doc(string(in), info);
  }

  @Override
  public final boolean accept(final ASTVisitor visitor) {
    if(exprs.length == 0) {
      if(oneOf(sig, COLLECTION, URI_COLLECTION) && !visitor.lock(COLL)) return false;
    } else if(!(exprs[0] instanceof Str)) {
      if(!visitor.lock(null)) return false;
    } else {
      final QueryInput qi = new QueryInput(string(((Str) exprs[0]).string()));
      if(!visitor.lock(qi.db)) return false;
    }
    return super.accept(visitor);
  }
}

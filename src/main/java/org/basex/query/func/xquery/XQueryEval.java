package org.basex.query.func.xquery;

import org.basex.query.QueryContext;
import org.basex.query.QueryException;
import org.basex.query.StaticContext;
import org.basex.query.func.StandardFunc;
import org.basex.query.iter.Iter;
import org.basex.query.util.ASTVisitor;
import org.basex.query.util.list.ItemList;
import org.basex.query.value.Value;
import org.basex.query.value.item.QNm;
import org.basex.util.Performance;
import org.basex.util.options.EnumOption;
import org.basex.util.options.NumberOption;
import org.basex.util.options.Options;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;

import static org.basex.query.QueryError.BASX_PERM_X;
import static org.basex.query.QueryError.BXXQ_NOUPDATE;
import static org.basex.query.QueryError.BXXQ_PERM2_X;
import static org.basex.query.QueryError.BXXQ_PERM_X;
import static org.basex.query.QueryError.BXXQ_STOPPED;
import static org.basex.query.QueryError.BXXQ_UPDATING;
import static org.basex.query.QueryText.XQUERY_PREFIX;
import static org.basex.query.QueryText.XQUERY_URI;
import static org.basex.util.Token.string;

/**
 * Function implementation.
 *
 * @author BaseX Team 2005-15, BSD License
 * @author Christian Gruen
 */
public class XQueryEval extends StandardFunc {
  /** QName. */
  private static final QNm Q_OPTIONS = QNm.get(XQUERY_PREFIX, "options", XQUERY_URI);

  /** XQuery options. */
  public static class XQueryOptions extends Options {
    /** Permission. */
//    public static final EnumOption<Perm> PERMISSION = new EnumOption<>("permission", Perm.ADMIN);
    /** Timeout in seconds. */
    public static final NumberOption TIMEOUT = new NumberOption("timeout", 0);
    /** Maximum amount of megabytes that may be allocated by the query. */
    public static final NumberOption MEMORY = new NumberOption("memory", 0);
  }

  @Override
  public final Iter iter(final QueryContext qc) throws QueryException {
    return eval(qc).iter();
  }

  @Override
  public final Value value(final QueryContext qc) throws QueryException {
    return eval(qc).value();
  }

  /**
   * Evaluates a query.
   * @param qc query context
   * @return resulting value
   * @throws QueryException query exception
   */
  protected ItemList eval(final QueryContext qc) throws QueryException {
    return eval(qc, toToken(exprs[0], qc), null, false);
  }

  /**
   * Evaluates the specified string.
   * @param qc query context
   * @param qu query string
   * @param path path to query file (may be {@code null})
   * @param updating updating query
   * @return resulting value
   * @throws QueryException query exception
   */
  final ItemList eval(final QueryContext qc, final byte[] qu, final String path,
      final boolean updating) throws QueryException {

    // bind variables and context value
    final HashMap<String, Value> bindings = toBindings(1, qc);
//    final User user = qc.context.user();
//    final Perm tmp = user.perm("");
    Timer to = null;

    try(final QueryContext qctx = new QueryContext(qc)) {
      if(exprs.length > 2) {
        final Options opts = toOptions(2, Q_OPTIONS, new XQueryOptions(), qc);
//        final Perm perm = Perm.get(opts.get(XQueryOptions.PERMISSION).toString());
//        if(!user.has(perm)) throw BXXQ_PERM2_X.get(info, perm);
//        user.perm(perm, "");

        // initial memory consumption: perform garbage collection and calculate usage
        final long mb = opts.get(XQueryOptions.MEMORY);
        if(mb != 0) {
          final long limit = Performance.memory() + (mb << 20);
          to = new Timer(true);
          to.schedule(new TimerTask() {
            @Override
            public void run() {
              // limit reached: perform garbage collection and check again
              if(Performance.memory() > limit) System.err.println("EVALUATING FOR TOO LONG!"); //qctx.stop();
            }
          }, 500, 500);
        }
//        final long ms = opts.get(XQueryOptions.TIMEOUT) * 1000L;
//        if(ms != 0) {
//          if(to == null) to = new Timer(true);
//          to.schedule(new TimerTask() {
//            @Override
//            public void run() { qctx.stop(); }
//          }, ms);
//        }
      }

      // evaluate query
      try {
        final StaticContext sctx = new StaticContext(qctx);
        for(final Entry<String, Value> it : bindings.entrySet()) {
          final String key = it.getKey();
          final Value val = it.getValue();
          if(key.isEmpty()) qctx.context(val, sctx);
          else qctx.bind(key, val, sctx);
        }
        qctx.parseMain(string(qu), path, sctx);

        if(updating) {
          if(!sc.mixUpdates && !qctx.updating && !qctx.root.expr.isVacuous())
            throw BXXQ_NOUPDATE.get(info);
        } else {
          if(qctx.updating) throw BXXQ_UPDATING.get(info);
        }

        final ItemList cache = new ItemList();
        cache(qctx.iter(), cache, qctx);
        return cache;
//      } catch(final ProcException ex) {
//        throw BXXQ_STOPPED.get(info);
      } catch(final QueryException ex) {
        throw ex.error() == BASX_PERM_X ? BXXQ_PERM_X.get(info, ex.getLocalizedMessage()) :
          ex.info(info);
      }

    } finally {
//      user.perm(tmp, "");
//      qc.proc(null);
      if(to != null) to.cancel();
    }
  }

  @Override
  public boolean accept(final ASTVisitor visitor) {
    return visitor.lock(null) && super.accept(visitor);
  }
}

package org.basex.query.func.xquery;

import org.basex.query.LibraryModule;
import org.basex.query.QueryContext;
import org.basex.query.QueryException;
import org.basex.query.QueryText;
import org.basex.query.StaticScope;
import org.basex.query.func.StandardFunc;
import org.basex.query.value.item.QNm;
import org.basex.query.value.node.FElem;
import org.basex.util.InputInfo;
import org.basex.util.options.BooleanOption;
import org.basex.util.options.Options;

import static org.basex.util.Token.string;
import static org.basex.util.Token.token;

/**
 * Function implementation.
 *
 * @author BaseX Team 2005-15, BSD License
 * @author Christian Gruen
 */
public final class XQueryParse extends StandardFunc {
  /** Token. */
  private static final byte[] LIBRARY_MODULE = token("LibraryModule");
  /** Token. */
  private static final byte[] MAIN_MODULE = token("MainModule");
  /** Token. */
  private static final byte[] UPDATING = token("updating");
  /** Token. */
  private static final byte[] PREFIX = token("prefix");
  /** Token. */
  private static final byte[] URI = token("uri");

  /** QName. */
  private static final QNm Q_OPTIONS = QNm.get(QueryText.XQUERY_PREFIX, "options",
    QueryText.XQUERY_URI);

  /** XQuery options. */
  public static class XQueryOptions extends Options {
    /** Return plan. */
    public static final BooleanOption PLAN = new BooleanOption("plan", true);
    /** Compile query. */
    public static final BooleanOption COMPILE = new BooleanOption("compile", false);
  }

  @Override
  public FElem item(final QueryContext qc, final InputInfo ii) throws QueryException {
    final byte[] query = toToken(exprs[0], qc);

    boolean compile = false, plan = true;
    if(exprs.length > 1) {
      final Options opts = toOptions(1, Q_OPTIONS, new XQueryOptions(), qc);
      compile = opts.get(XQueryOptions.COMPILE);
      plan = opts.get(XQueryOptions.PLAN);
    }

    try(final QueryContext qctx = new QueryContext(qc)) {
      final StaticScope ss = qctx.parse(string(query), null, null);
      final boolean library = ss instanceof LibraryModule;

      final FElem root;
      if(library) {
        root = new FElem(LIBRARY_MODULE);
        final LibraryModule lib = (LibraryModule) ss;
        root.add(PREFIX, lib.name.string());
        root.add(URI, lib.name.uri());
      } else {
        root = new FElem(MAIN_MODULE);
        root.add(UPDATING, token(qctx.updating));
      }

      if(compile) qctx.compile();
      if(plan) root.add(qctx.plan());
      return root;
    } catch(final QueryException ex) {
      throw ex.info(info);
    }
  }
}

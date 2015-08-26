package org.basex.query.up.primitives.db;

import static org.basex.query.QueryError.*;

import java.io.*;
import java.util.*;

import org.basex.core.*;
import org.basex.core.cmd.*;
import org.basex.data.*;
import org.basex.query.*;
import org.basex.query.up.primitives.*;
import org.basex.util.*;
import org.basex.util.ft.*;
import org.basex.util.options.*;

/**
 * Update primitive for the optimize function.
 *
 * @author BaseX Team 2005-15, BSD License
 * @author Dimitar Popov
 */
public final class DBOptimize extends DBUpdate {
  /** Options supplied with the function call. */
  private final DBOptions options;
  /** Query context. */
  private final QueryContext qc;
  /** Flag to optimize all database structures. */
  private boolean all;

  /**
   * Constructor.
   * @param data data
   * @param all optimize all database structures flag
   * @param opts database options
   * @param qc database context
   * @param info input info
   * @throws QueryException query exception
   */
  public DBOptimize(final Data data, final boolean all, final Options opts, final QueryContext qc,
      final InputInfo info) throws QueryException {

    super(UpdateType.DBOPTIMIZE, data, info);
    this.all = all;
    this.qc = qc;

    final ArrayList<Option<?>> supported = new ArrayList<>();
    for(final Option<?> option : DBOptions.INDEXING) {
      if(all || option != MainOptions.UPDINDEX) supported.add(option);
    }
    options = new DBOptions(opts, supported, info);
  }

  @Override
  public void merge(final Update update) {
    all |= ((DBOptimize) update).all;
  }

  @Override
  public void prepare(final MemData tmp) { }

  @Override
  public void apply() throws QueryException {
    // create new options, based on global defaults, and overwrite with database options
    final MainOptions opts = new MainOptions(qc.context.options);
    final MetaData meta = data.meta;
    options.assign(MainOptions.TEXTINDEX,    meta.createtext);
    options.assign(MainOptions.ATTRINDEX,    meta.createattr);
    options.assign(MainOptions.FTINDEX,      meta.createftxt);
    options.assign(MainOptions.TEXTINCLUDE,  meta.textinclude);
    options.assign(MainOptions.ATTRINCLUDE,  meta.attrinclude);
    options.assign(MainOptions.FTINCLUDE,    meta.ftinclude);
    options.assign(MainOptions.UPDINDEX,     meta.updindex);
    options.assign(MainOptions.AUTOOPTIMIZE, meta.autoopt);
    options.assignTo(opts);

    // adopt options to database meta data
    meta.createtext = opts.get(MainOptions.TEXTINDEX);
    meta.createattr = opts.get(MainOptions.ATTRINDEX);
    meta.createftxt = opts.get(MainOptions.FTINDEX);
    meta.updindex = opts.get(MainOptions.UPDINDEX);

    // check if other indexing options have changed
    final int mc = opts.get(MainOptions.MAXCATS);
    final int ml = opts.get(MainOptions.MAXLEN);
    final String ti = opts.get(MainOptions.TEXTINCLUDE);
    final String ai = opts.get(MainOptions.ATTRINCLUDE);
    final boolean rebuild = mc != meta.maxcats || ml != meta.maxlen;
    final boolean rebuildText = !meta.textinclude.equals(ti);
    final boolean rebuildAttr = !meta.attrinclude.equals(ai);
    meta.textinclude = ti;
    meta.attrinclude = ai;
    meta.maxcats = mc;
    meta.maxlen = ml;

    // check if fulltext indexing options have changed
    final boolean st = opts.get(MainOptions.STEMMING);
    final boolean cs = opts.get(MainOptions.CASESENS);
    final boolean dc = opts.get(MainOptions.DIACRITICS);
    final String sw = opts.get(MainOptions.STOPWORDS);
    final Language ln = Language.get(opts);
    final String fi = opts.get(MainOptions.FTINCLUDE);
    final boolean rebuildFtx = rebuild || !ln.equals(meta.language) || st != meta.stemming ||
        cs != meta.casesens || dc != meta.diacritics || !sw.equals(meta.stopwords) ||
        !meta.ftinclude.equals(fi);
    meta.language   = ln;
    meta.stemming   = st;
    meta.casesens   = cs;
    meta.diacritics = dc;
    meta.stopwords  = sw;
    meta.ftinclude = fi;

    try {
      if(all) OptimizeAll.optimizeAll(data, qc.context, opts, null);
      else Optimize.optimize(data, opts, rebuildText, rebuildAttr, rebuildFtx, null);
    } catch(final IOException ex) {
      throw UPDBOPTERR_X.get(info, ex);
    }

    // remove old database reference
    if(all) qc.resources.remove(meta.name);
  }

  @Override
  public int size() {
    return 1;
  }
}

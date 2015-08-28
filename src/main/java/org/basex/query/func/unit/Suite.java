package org.basex.query.func.unit;

import org.basex.core.MainOptions;
import org.basex.io.IO;
import org.basex.io.IOFile;
import org.basex.query.value.node.FElem;
import org.basex.util.Performance;

import java.io.IOException;
import java.util.ArrayList;

import static org.basex.query.func.unit.Constants.TESTSUITES;
import static org.basex.query.func.unit.Constants.TIME;

/**
 * XQUnit tests: Testing multiple modules.
 *
 * @author BaseX Team 2005-15, BSD License
 * @author Christian Gruen
 */
public final class Suite {
  /** Failures. */
  public int failures;
  /** Errors. */
  public int errors;
  /** Skipped. */
  public int skipped;
  /** Tests. */
  public int tests;

  /**
   * Tests all test functions in the specified path.
//   * @param ctx database context
   * @param root path to test modules
//   * @param proc calling process
   * @return resulting value
   * @throws IOException I/O exception
   */
  public FElem test(final IOFile root, final MainOptions opt) throws IOException {
    final ArrayList<IOFile> files = new ArrayList<>();

    final Performance perf = new Performance();
    final FElem suites = new FElem(TESTSUITES);
    if(root.isDir()) {
      for(final String path : root.descendants()) {
        final IOFile file = new IOFile(root, path);
        if(file.hasSuffix(IO.XQSUFFIXES)) files.add(file);
      }
    } else {
      files.add(root);
    }

    for(final IOFile file : files) {
      final Unit unit = new Unit(file, opt);
      unit.test(suites);
      errors += unit.errors;
      failures += unit.failures;
      skipped += unit.skipped;
      tests += unit.tests;
    }

    suites.add(TIME, Unit.time(perf));
    return suites;
  }
}

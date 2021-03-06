package org.basex.query.expr.ft;

import org.basex.core.MainOptions;
import org.basex.io.IO;
import org.basex.query.QueryException;
import org.basex.query.QueryProcessor;
import org.basex.query.value.Value;
import org.basex.query.value.item.Item;
import org.basex.query.value.node.DBNode;
import org.basex.util.Array;
import org.basex.util.InputInfo;
import org.basex.util.hash.TokenMap;
import org.basex.util.hash.TokenObjMap;
import org.basex.util.list.TokenList;

import java.io.IOException;

import static org.basex.query.QueryError.NOTHES_X;
import static org.basex.util.Token.EMPTY;
import static org.basex.util.Token.eq;

/**
 * Simple Thesaurus for full-text requests.
 *
 * @author BaseX Team 2005-15, BSD License
 * @author Christian Gruen
 */
public final class Thesaurus {
  /** Thesaurus root references. */
  private final TokenObjMap<ThesNode> nodes = new TokenObjMap<>();
  /** Relationships. */
  private static final TokenMap RSHIPS = new TokenMap();
  /** Database context. */
//  private final Context ctx;
  private MainOptions options;

  static {
    RSHIPS.put("NT", "BT");
    RSHIPS.put("BT", "BT");
    RSHIPS.put("BTG", "NTG");
    RSHIPS.put("NTG", "BTG");
    RSHIPS.put("BTP", "NTP");
    RSHIPS.put("NTP", "BTP");
    RSHIPS.put("USE", "UF");
    RSHIPS.put("UF", "USE");
    RSHIPS.put("RT", "RT");
  }

  /** Thesaurus node. */
  private static class ThesNode {
    /** Related nodes. */
    private ThesNode[] nodes = new ThesNode[1];
    /** Relationships. */
    private byte[][] rs = new byte[1][];
    /** Term. */
    private byte[] term;
    /** Entries. */
    private int size;

    /**
     * Adds a relationship to the node.
     * @param n target node
     * @param r relationship
     */
    private void add(final ThesNode n, final byte[] r) {
      if(size == nodes.length) {
        final int s = Array.newSize(size);
        nodes = Array.copy(nodes, new ThesNode[s]);
        rs = Array.copyOf(rs, s);
      }
      nodes[size] = n;
      rs[size++] = r;
    }
  }

  /** Input file. */
  private final IO file;
  /** Relationship. */
  private final byte[] rel;
  /** Minimum level. */
  private final long min;
  /** Maximum level. */
  private final long max;

  /**
   * Constructor.
   * @param file file reference
//   * @param ctx database context
   */
  public Thesaurus(final IO file, final MainOptions options) {
    this(file, EMPTY, 0, Long.MAX_VALUE, options);
  }

  /**
   * Reads a thesaurus file.
   * @param file file reference
   * @param res relationship
   * @param min minimum level
   * @param max maximum level
//   * @param ctx database context
   */
  public Thesaurus(final IO file, final byte[] res, final long min, final long max,
                   final MainOptions options) {
    this.file = file;
    rel = res;
    this.min = min;
    this.max = max;
    this.options = options;
  }

  /**
   * Initializes the thesaurus.
   * @param ii input info
   * @throws QueryException query exception
   */
  private void init(final InputInfo ii) throws QueryException {
    try {
      final Value entries = nodes("//*:entry", new DBNode(file));
      for(final Item entry : entries) build(entry);
    } catch(final IOException ex) {
      throw NOTHES_X.get(ii, file);
    }
  }

  /**
   * Builds the thesaurus.
   * @param value input nodes
   * @throws QueryException query exception
   */
  private void build(final Value value) throws QueryException {
    final Value synonyms = nodes("*:synonym", value);
    if(synonyms.isEmpty()) return;

    final ThesNode term = node(text("*:term", value));
    for(final Item synonym : synonyms) {
      final ThesNode sterm = node(text("*:term", synonym));
      final byte[] rs = text("*:relationship", synonym);
      term.add(sterm, rs);

      final byte[] srs = RSHIPS.get(rs);
      if(srs != null) sterm.add(term, srs);
      build(synonyms);
    }
  }

  /**
   * Returns a node for the specified term.
   * @param term term
   * @return node
   */
  private ThesNode node(final byte[] term) {
    ThesNode node = nodes.get(term);
    if(node == null) {
      node = new ThesNode();
      node.term = term;
      nodes.put(term, node);
    }
    return node;
  }

  /**
   * Performs a query and returns the result as nodes.
   * @param query query string
   * @param value value
   * @return resulting nodes
   * @throws QueryException query exception
   */
  private Value nodes(final String query, final Value value) throws QueryException {
    try(final QueryProcessor qp = new QueryProcessor(query, options).context(value)) {
      return qp.value();
    } catch(IOException ioe) {
      throw new QueryException(ioe);
    }
  }

  /**
   * Performs a query and returns the first result as text.
   * @param query query string
   * @param value value
   * @return resulting text
   * @throws QueryException query exception
   */
  private byte[] text(final String query, final Value value) throws QueryException {
    try(final QueryProcessor qp = new QueryProcessor(query, options).context(value)) {
      return qp.iter().next().string(null);
    } catch(IOException ioe) {
      throw new QueryException(ioe);
    }
  }

  /**
   * Finds a thesaurus term.
   * @param ii input info
   * @param list result list
   * @param token token
   * @throws QueryException query exception
   */
  void find(final InputInfo ii, final TokenList list, final byte[] token) throws QueryException {
    if(nodes.isEmpty()) init(ii);
    find(list, nodes.get(token), 1);
  }

  /**
   * Recursively collects relevant thesaurus terms.
   * @param list result list
   * @param node input node
   * @param level current level
   */
  private void find(final TokenList list, final ThesNode node, final long level) {
    if(level > max || node == null) return;

    for(int n = 0; n < node.size; ++n) {
      if(rel.length == 0 || eq(node.rs[n], rel)) {
        final byte[] term = node.nodes[n].term;
        if(!list.contains(term)) {
          list.add(term);
          find(list, node.nodes[n], level + 1);
        }
      }
    }
  }

  /**
   * Compares two thesaurus instances.
   * @param th instance to be compared
   * @return result of check
   */
  boolean sameAs(final Thesaurus th) {
    return file.eq(th.file) && min == th.min && max == th.max && eq(rel, th.rel);
  }
}

package org.basex.query.func;

import org.basex.core.MainOptions;
import org.basex.query.QueryContext;
import org.basex.query.QueryException;
import org.basex.query.Scope;
import org.basex.query.StaticContext;
import org.basex.query.StaticDecl;
import org.basex.query.ann.Annotation;
import org.basex.query.expr.Expr;
import org.basex.query.expr.Expr.Flag;
import org.basex.query.expr.ParseExpr;
import org.basex.query.expr.TypeCheck;
import org.basex.query.expr.XQFunction;
import org.basex.query.expr.gflwor.GFLWOR;
import org.basex.query.expr.gflwor.GFLWOR.Clause;
import org.basex.query.expr.gflwor.Let;
import org.basex.query.func.fn.FnError;
import org.basex.query.util.ASTVisitor;
import org.basex.query.util.Ann;
import org.basex.query.util.list.AnnList;
import org.basex.query.value.Value;
import org.basex.query.value.item.ANum;
import org.basex.query.value.item.Item;
import org.basex.query.value.item.QNm;
import org.basex.query.value.node.FElem;
import org.basex.query.value.type.AtomType;
import org.basex.query.value.type.FuncType;
import org.basex.query.value.type.SeqType;
import org.basex.query.var.Var;
import org.basex.query.var.VarScope;
import org.basex.util.InputInfo;
import org.basex.util.TokenBuilder;
import org.basex.util.hash.IntObjMap;

import java.util.EnumMap;
import java.util.LinkedList;

import static org.basex.query.QueryError.UPEXPECTF;
import static org.basex.query.QueryError.UPNOT_X;
import static org.basex.query.QueryError.UUPFUNCTYPE;
import static org.basex.query.QueryText.ARG;
import static org.basex.query.QueryText.AS;
import static org.basex.query.QueryText.DECLARE;
import static org.basex.query.QueryText.FUNCTION;
import static org.basex.query.QueryText.NAM;
import static org.basex.query.QueryText.OPTCAST;
import static org.basex.query.QueryText.OPTINLINE;
import static org.basex.query.QueryText.PAREN1;
import static org.basex.query.QueryText.PAREN2;
import static org.basex.query.QueryText.SEP;

/**
 * A static user-defined function.
 *
 * @author BaseX Team 2005-15, BSD License
 * @author Leo Woerteler
 */
public final class StaticFunc extends StaticDecl implements XQFunction {
  /** Arguments. */
  public final Var[] args;
  /** Updating flag. */
  final boolean updating;

  /** Map with requested function properties. */
  private final EnumMap<Flag, Boolean> map = new EnumMap<>(Flag.class);
  /** Flag that is turned on during compilation and prevents premature inlining. */
  private boolean compiling;

  /**
   * Function constructor.
   * @param anns annotations
   * @param name function name
   * @param args arguments
   * @param type declared return type
   * @param expr function body
   * @param sc static context
   * @param scope variable scope
   * @param doc current xqdoc cache
   * @param info input info
   */
  StaticFunc(final AnnList anns, final QNm name, final Var[] args, final SeqType type,
      final Expr expr, final StaticContext sc, final VarScope scope, final String doc,
      final InputInfo info) {

    super(sc, anns, name, type, scope, doc, info);
    this.args = args;
    this.expr = expr;
    updating = anns.contains(Annotation.UPDATING);
  }

  @Override
  public void compile(final QueryContext qc) {
    if(compiled) return;
    compiling = compiled = true;

    final Value cv = qc.value;
    qc.value = null;

    try {
      expr = expr.compile(qc, scope);

      if(declType != null) {
        // remove redundant casts
        if((declType.type == AtomType.BLN || declType.type == AtomType.FLT ||
            declType.type == AtomType.DBL || declType.type == AtomType.QNM ||
            declType.type == AtomType.URI) && declType.eq(expr.seqType())) {
          qc.compInfo(OPTCAST, declType);
        } else {
          expr = new TypeCheck(sc, info, expr, declType, true).optimize(qc, scope);
        }
      }
    } catch(final QueryException qe) {
      expr = FnError.get(qe, expr.seqType());
    } finally {
      scope.cleanUp(this);
      qc.value = cv;
    }

    // convert all function calls in tail position to proper tail calls
    expr.markTailCalls(qc);

    compiling = false;
  }

  @Override
  public void plan(final FElem plan) {
    final FElem el = planElem(NAM, name.string());
    addPlan(plan, el, expr);
    final int al = args.length;
    for(int a = 0; a < al; ++a) el.add(planAttr(ARG + a, args[a].name.string()));
  }

  @Override
  public String toString() {
    final TokenBuilder tb = new TokenBuilder(DECLARE).add(' ').addExt(anns);
    tb.add(FUNCTION).add(' ').add(name.string());
    tb.add(PAREN1).addSep(args, SEP).add(PAREN2);
    if(declType != null) tb.add(' ' + AS + ' ' + declType);
    if(expr != null) tb.add(" { ").addExt(expr).add(" }; ");
    else tb.add(" external; ");
    return tb.toString();
  }

  /**
   * Checks if this function calls itself recursively.
   * @return result of check
   */
  private boolean selfRecursive() {
    return !expr.accept(new ASTVisitor() {
      @Override
      public boolean staticFuncCall(final StaticFuncCall call) {
        return call.func != StaticFunc.this;
      }

      @Override
      public boolean inlineFunc(final Scope sub) {
        return sub.visit(this);
      }
    });
  }

  @Override
  public int arity() {
    return args.length;
  }

  @Override
  public QNm funcName() {
    return name;
  }

  @Override
  public QNm argName(final int pos) {
    return args[pos].name;
  }

  @Override
  public FuncType funcType() {
    return FuncType.get(anns, args, declType);
  }

  @Override
  public int stackFrameSize() {
    return scope.stackSize();
  }

  @Override
  public AnnList annotations() {
    return anns;
  }

  @Override
  public Item invItem(final QueryContext qc, final InputInfo ii, final Value... arg)
      throws QueryException {

    // reset context and evaluate function
    final Value cv = qc.value;
    qc.value = null;
    try {
      final int al = args.length;
      for(int a = 0; a < al; a++) qc.set(args[a], arg[a], ii);
      return expr.item(qc, ii);
    } finally {
      qc.value = cv;
    }
  }

  @Override
  public Value invValue(final QueryContext qc, final InputInfo ii, final Value... arg)
      throws QueryException {

    // reset context and evaluate function
    final Value cv = qc.value;
    qc.value = null;
    try {
      final int al = args.length;
      for(int a = 0; a < al; a++) qc.set(args[a], arg[a], ii);
      return qc.value(expr);
    } finally {
      qc.value = cv;
    }
  }

  @Override
  public Value invokeValue(final QueryContext qc, final InputInfo ii, final Value... arg)
      throws QueryException {
    return FuncCall.value(this, arg, qc, ii);
  }

  @Override
  public Item invokeItem(final QueryContext qc, final InputInfo ii, final Value... arg)
      throws QueryException {
    return FuncCall.item(this, arg, qc, ii);
  }

  /**
   * Checks if all updating expressions in the function are correctly declared and placed.
   * @throws QueryException query exception
   */
  void checkUp() throws QueryException {
    final boolean u = expr.has(Flag.UPD);
    if(u) expr.checkUp();
    final InputInfo ii = expr instanceof ParseExpr ? ((ParseExpr) expr).info : info;
    if(updating) {
      // updating function
      if(declType != null) throw UUPFUNCTYPE.get(info);
      if(!u && !expr.isVacuous()) throw UPEXPECTF.get(ii);
    } else if(u) {
      // uses updates, but is not declared as such
      throw UPNOT_X.get(ii, description());
    }
  }

  /**
   * Checks if this function returns vacuous results (see {@link Expr#isVacuous()}).
   * @return result of check
   */
  public boolean isVacuous() {
    return !has(Flag.UPD) && declType != null && declType.eq(SeqType.EMP);
  }

  /**
   * Indicates if an expression has the specified compiler property.
   * @param flag feature
   * @return result of check
   * @see Expr#has(Flag)
   */
  boolean has(final Flag flag) {
    // handle recursive calls: set dummy value, eventually replace it with final value
    Boolean b = map.get(flag);
    if(b == null) {
      map.put(flag, false);
      b = expr == null || expr.has(flag);
      map.put(flag, b);
    }
    return b;
  }

  @Override
  public boolean visit(final ASTVisitor visitor) {
    for(final Var v : args) if(!visitor.declared(v)) return false;
    return expr.accept(visitor);
  }

  @Override
  public byte[] id() {
    return StaticFuncs.sig(name, args.length);
  }

  @Override
  public Expr inlineExpr(final Expr[] exprs, final QueryContext qc, final VarScope scp,
      final InputInfo ii) throws QueryException {

    if(!inline(qc, anns, expr) || has(Flag.CTX) || compiling || selfRecursive()) return null;
    qc.compInfo(OPTINLINE, id());

    // create let bindings for all variables
    final LinkedList<Clause> cls = exprs.length == 0 ? null : new LinkedList<Clause>();
    final IntObjMap<Var> vs = new IntObjMap<>();
    final int al = args.length;
    for(int a = 0; a < al; a++) {
      final Var old = args[a], v = scp.newCopyOf(qc, old);
      vs.put(old.id, v);
      cls.add(new Let(v, exprs[a], false, info).optimize(qc, scp));
    }

    // copy the function body
    final Expr cpy = expr.copy(qc, scp, vs);
    return cls == null ? cpy : new GFLWOR(info, cls, cpy).optimize(qc, scp);
  }

  /**
   * Checks if inlining conditions are given.
   * @param qc query context
   * @param anns annotations
   * @param expr expression
   * @return result of check
   */
  public static boolean inline(final QueryContext qc, final AnnList anns, final Expr expr) {
    final Ann ann = anns.get(Annotation._BASEX_INLINE);
    final long limit = ann != null
        ? ann.args.length > 0 ? ((ANum) ann.args[0]).itr() : Long.MAX_VALUE
        : qc.options.get(MainOptions.INLINELIMIT);
    return expr.isValue() || expr.exprSize() < limit;
  }
}

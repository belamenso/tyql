package tyql

/** Nodes in the query IR tree, representing expressions / subclauses
  */
trait QueryIRNode:
  val ast
    : DatabaseAST[?] | Expr[?, ?] | Expr.Fun[
      ?,
      ?,
      ?
    ] | UpdateToTheDB // Best-effort, keep AST around for debugging, TODO: probably remove, or replace only with ResultTag

  val precedence: Int = Precedence.Default
  private var cached: java.util.concurrent.ConcurrentHashMap[(Dialect, Config), SQLRenderingContext] =
    null // do not allocate memory if unused

  final def toSQLString()(using d: Dialect)(using cnf: Config): String =
    val (sql, _) = toSQLQuery()
    sql

  // TODO maybe private?
  final def toSQLQuery(using d: Dialect)(using cnf: Config)(): (String, collection.mutable.ArrayBuffer[Object]) =
    if cached == null then
      this.synchronized {
        if cached == null then
          cached = new java.util.concurrent.ConcurrentHashMap[(Dialect, Config), SQLRenderingContext]()
      }
    val ctx = cached.computeIfAbsent(
      (d, cnf),
      _ =>
        val ctx = new SQLRenderingContext()
        computeSQL(using d)(using cnf)(ctx)
        ctx
    )
    (ctx.sql.toString, ctx.parameters)

  private[tyql] def computeSQL(using d: Dialect)(using cnf: Config)(ctx: SQLRenderingContext): Unit

trait QueryIRLeaf extends QueryIRNode

/** Single WHERE clause containing 1+ predicates
  */
case class WhereClause(children: Seq[QueryIRNode], ast: Expr[?, ?]) extends QueryIRNode:
  override def computeSQL(using d: Dialect)(using cnf: Config)(ctx: SQLRenderingContext): Unit =
    if children.size == 1 then
      children.head.computeSQL(ctx)
    else
      ctx.mkString(children, " AND ")

/** Binary expression-level operation. TODO: cannot assume the operation is universal, need to specialize for DB backend
  */
case class BinExprOp
  (
      pre: String,
      lhs: QueryIRNode,
      mid: String,
      rhs: QueryIRNode,
      post: String,
      override val precedence: Int,
      ast: Expr[?, ?]
  ) extends QueryIRNode:
  override def computeSQL(using d: Dialect)(using cnf: Config)(ctx: SQLRenderingContext): Unit =
    ctx.sql.append(pre)
    if needsParens(lhs) then
      ctx.sql.append("(")
      lhs.computeSQL(ctx)
      ctx.sql.append(")")
    else
      lhs.computeSQL(ctx)
    ctx.sql.append(mid)
    if needsParens(rhs) then
      ctx.sql.append("(")
      rhs.computeSQL(ctx)
      ctx.sql.append(")")
    else
      rhs.computeSQL(ctx)
    ctx.sql.append(post)

  private def needsParens(node: QueryIRNode): Boolean =
    // TODO should this be specialized into needsLeftParen and needsRightParen?
    //      Unclear if it would be used since we generate from Scala expressions...
    node.precedence != 0 && node.precedence < this.precedence

/** Unary expression-level operation. TODO: cannot assume the operation is universal, need to specialize for DB backend
  */
case class UnaryExprOp(pre: String, child: QueryIRNode, post: String, ast: Expr[?, ?]) extends QueryIRNode:
  override val precedence: Int = Precedence.Unary
  override def computeSQL(using d: Dialect)(using cnf: Config)(ctx: SQLRenderingContext): Unit =
    ctx.sql.append(pre)
    child.computeSQL(ctx)
    ctx.sql.append(post)

case class FunctionCallOp(name: String, children: Seq[QueryIRNode], ast: Expr[?, ?], includeParens: Boolean = true)
    extends QueryIRNode:
  override val precedence = Precedence.Literal
  override def computeSQL(using d: Dialect)(using cnf: Config)(ctx: SQLRenderingContext): Unit =
    ctx.sql.append(name)
    if includeParens then
      ctx.mkString(children, "(", ", ", ")")
    else
      assert(children.isEmpty)
      ctx.mkString(children, "", ", ", "")

/** CASE statement called "searched" in the standard. This is like a multi-arm if-else statement.
  */
case class SearchedCaseOp
  (whenClauses: Seq[(QueryIRNode, QueryIRNode)], elseClause: Option[QueryIRNode], ast: Expr[?, ?]) extends QueryIRNode:
  override val precedence = Precedence.Literal
  override def computeSQL(using d: Dialect)(using cnf: Config)(ctx: SQLRenderingContext): Unit =
    ctx.sql.append("CASE ")
    var first = true
    for (cond, res) <- whenClauses do
      if first then first = false else ctx.sql.append(" ")
      ctx.sql.append("WHEN ")
      cond.computeSQL(ctx)
      ctx.sql.append(" THEN ")
      res.computeSQL(ctx)
    if elseClause.nonEmpty then
      ctx.sql.append(" ELSE ")
      elseClause.get.computeSQL(ctx)
    ctx.sql.append(" END")

/** CASE statement called "simple" in the standard. This is a vague equivalent of Scala's match expression.
  */
case class SimpleCaseOp
  (expr: QueryIRNode, whenClauses: Seq[(QueryIRNode, QueryIRNode)], elseClause: Option[QueryIRNode], ast: Expr[?, ?])
    extends QueryIRNode:
  override val precedence = Precedence.Literal
  override def computeSQL(using d: Dialect)(using cnf: Config)(ctx: SQLRenderingContext): Unit =
    ctx.sql.append("CASE ")
    expr.computeSQL(ctx)
    ctx.sql.append(" ")
    var first = true
    for (cond, res) <- whenClauses do
      if first then first = false else ctx.sql.append(" ")
      ctx.sql.append("WHEN ")
      cond.computeSQL(ctx)
      ctx.sql.append(" THEN ")
      res.computeSQL(ctx)
    if elseClause.nonEmpty then
      ctx.sql.append(" ELSE ")
      elseClause.get.computeSQL(ctx)
    ctx.sql.append(" END")

/** For when we include something that does not make sense to represent fully in the IR, for example some one-off
  * per-dialect features.
  */
case class RawSQLInsertOp
  (snippet: SqlSnippet, replacements: Map[String, QueryIRNode], override val precedence: Int, ast: Expr[?, ?])
    extends QueryIRNode:
  override def computeSQL(using d: Dialect)(using cnf: Config)(ctx: SQLRenderingContext): Unit =
    assert(replacements.keySet == snippet.sql.filter {
      case (s: String) => false; case (name: String, prec: Int) => true
    }.map { case (name: String, prec: Int) => name; case _ => assert(false) }.toSet)
    assert(precedence == snippet.precedence)
    snippet.sql.map {
      case s: String => ctx.sql.append(s)
      case (name: String, placementPrecedence: Int) =>
        val innerPrecedence = replacements(name).precedence
        if innerPrecedence <= placementPrecedence then
          ctx.sql.append("(")
          replacements(name).computeSQL(ctx)
          ctx.sql.append(")")
        else
          replacements(name).computeSQL(ctx)
    }.mkString

/** Variable inputs so we can utilize the cache between different invocations */
case class VariableInputOp[T](vi: Expr.VariableInput[T], val ast: Expr[?, ?]) extends QueryIRNode:
  override val precedence: Int = Precedence.Literal
  override def computeSQL(using d: Dialect)(using cnf: Config)(ctx: SQLRenderingContext): Unit =
    cnf.parameterStyle match
      case ParameterStyle.EscapedInline =>
        val exprHere = vi.evaluateToLiteral()
        val qHere = QueryIRTree.generateExpr(exprHere, SymbolTable())
        qHere.computeSQL(ctx)
      case ParameterStyle.DriverParametrized =>
        ctx.sql.append(preferredPlaceholder(ctx, ctx.parameters.size + 1))
        ctx.parameters.append(vi)

case class WindowFunctionOp
  (expr: QueryIRNode, partitionBy: Seq[QueryIRNode], orderBy: Seq[(QueryIRNode, tyql.Ord)], ast: Expr[?, ?])
    extends QueryIRNode:
  override val precedence = Precedence.Literal
  override def computeSQL(using d: Dialect)(using cnf: Config)(ctx: SQLRenderingContext): Unit =
    expr.computeSQL(ctx)
    assert(partitionBy.nonEmpty || orderBy.nonEmpty)
    ctx.sql.append(" OVER ( ")

    var outputtedSomethingHere = false
    if partitionBy.nonEmpty then
      ctx.sql.append("PARTITION BY ")
      var first = true
      for e <- partitionBy do
        if first then first = false else ctx.sql.append(", ")
        outputtedSomethingHere = true
        e.computeSQL(ctx)
    if outputtedSomethingHere then
      ctx.sql.append(" ")
    if orderBy.nonEmpty then
      ctx.sql.append("ORDER BY ")
      var first = true
      for (e, ord) <- orderBy do
        if first then first = false else ctx.sql.append(", ")
        e.computeSQL(ctx)
        ord match
          case Ord.ASC  => ctx.sql.append(" ASC")
          case Ord.DESC => ctx.sql.append(" DESC")
    ctx.sql.append(")")

/** Project clause, e.g. SELECT <...> FROM
  * @param children
  * @param ast
  */
case class ProjectClause(children: Seq[QueryIRNode], ast: Expr[?, ?]) extends QueryIRNode:
  override def computeSQL(using d: Dialect)(using cnf: Config)(ctx: SQLRenderingContext): Unit =
    ctx.mkString(children, ", ")

/** Named or unnamed attribute select expression, e.g. `table.rowName as customName` TODO: generate anonymous names, or
  * allow generated queries to be unnamed, or only allow named tuple results? Note projected attributes with names is
  * not the same as aliasing, and just exists for readability
  */
case class AttrExpr(child: QueryIRNode, projectedName: Option[String], ast: Expr[?, ?])(using d: Dialect)
    extends QueryIRNode:
  override def computeSQL(using d: Dialect)(using cnf: Config)(ctx: SQLRenderingContext): Unit =
    child.computeSQL(ctx)
    projectedName match
      case Some(value) =>
        ctx.sql.append(" as ")
        ctx.sql.append(d.quoteIdentifier(value))
      case None => ()

/** Attribute access expression, e.g. `table.rowName`.
  */
case class SelectExpr(attrName: String, from: QueryIRNode, ast: Expr[?, ?]) extends QueryIRLeaf:
  override def computeSQL(using d: Dialect)(using cnf: Config)(ctx: SQLRenderingContext): Unit =
    from.computeSQL(ctx)
    ctx.sql.append(".")
    ctx.sql.append(d.quoteIdentifier(cnf.caseConvention.convert(attrName)))

/** A variable that points to a table or subquery.
  */
case class QueryIRVar(toSub: RelationOp, name: String, ast: Expr.Ref[?, ?]) extends QueryIRLeaf:
  override def computeSQL(using d: Dialect)(using cnf: Config)(ctx: SQLRenderingContext): Unit =
    ctx.sql.append(d.quoteIdentifier(cnf.caseConvention.convert(toSub.alias)))

  override def toString: String = s"VAR(${toSub.alias}.${name})"

/** Literals.
  */
private def preferredPlaceholder(using d: Dialect)(ctx: SQLRenderingContext, oneBasedNumber: Long): String =
  if d.`prefers $n over ? for parametrization` then
    "$" + oneBasedNumber
  else
    "?"

case class LiteralString(unescapedString: String, insideLikePatternQuoting: Boolean, ast: Expr[?, ?])
    extends QueryIRLeaf:
  override val precedence: Int = Precedence.Literal
  override def computeSQL(using d: Dialect)(using cnf: Config)(ctx: SQLRenderingContext): Unit =
    cnf.parameterStyle match
      case ParameterStyle.EscapedInline =>
        ctx.sql.append(d.quoteStringLiteral(unescapedString, insideLikePatternQuoting))
      case ParameterStyle.DriverParametrized =>
        ctx.sql.append(preferredPlaceholder(ctx, ctx.parameters.size + 1))
        ctx.parameters.append(unescapedString)

case class LiteralBytes(bytes: Array[Byte], ast: Expr[?, ?]) extends QueryIRLeaf:
  override val precedence: Int = Precedence.Literal
  override def computeSQL(using d: Dialect)(using cnf: Config)(ctx: SQLRenderingContext): Unit =
    cnf.parameterStyle match
      case ParameterStyle.EscapedInline =>
        d.needsByteByByteEscapingOfBlobs match
          case true  => ctx.sql.append(bytes.map(b => f"\\X${b}%02x").mkString("x'", "", "'") + "::BLOB")
          case false => ctx.sql.append(bytes.map(b => f"${b}%02x").mkString("x'", "", "'"))
      case ParameterStyle.DriverParametrized =>
        ctx.sql.append(preferredPlaceholder(ctx, ctx.parameters.size + 1))
        ctx.parameters.append(bytes.asInstanceOf[Object])

case class LiteralByteStream(bytes: () => java.io.InputStream, ast: Expr[?, ?]) extends QueryIRLeaf:
  override val precedence: Int = Precedence.Literal
  override def computeSQL(using d: Dialect)(using cnf: Config)(ctx: SQLRenderingContext): Unit =
    cnf.parameterStyle match
      case ParameterStyle.EscapedInline =>
        assert(false, "The whole point of byte streams is that they are streamed and not inlined")
      case ParameterStyle.DriverParametrized =>
        ctx.sql.append(preferredPlaceholder(ctx, ctx.parameters.size + 1))
        ctx.parameters.append(bytes.asInstanceOf[Object])

case class LiteralLocalDate(ld: java.time.LocalDate, ast: Expr[?, ?]) extends QueryIRLeaf:
  override val precedence: Int = Precedence.Literal
  override def computeSQL(using d: Dialect)(using cnf: Config)(ctx: SQLRenderingContext): Unit =
    cnf.parameterStyle match
      case ParameterStyle.EscapedInline =>
        if d.canExtractDateTimeComponentsNatively then
          ctx.sql.append("DATE'" + ld.format(java.time.format.DateTimeFormatter.ISO_DATE) + "'")
        else
          ctx.sql.append("'" + ld.format(java.time.format.DateTimeFormatter.ISO_DATE) + "'")
      case ParameterStyle.DriverParametrized =>
        ctx.sql.append(preferredPlaceholder(ctx, ctx.parameters.size + 1))
        ctx.parameters.append(ld.asInstanceOf[Object])

case class LiteralLocalDateTime(ldt: java.time.LocalDateTime, ast: Expr[?, ?]) extends QueryIRLeaf:
  override val precedence: Int = Precedence.Literal
  override def computeSQL(using d: Dialect)(using cnf: Config)(ctx: SQLRenderingContext): Unit =
    cnf.parameterStyle match
      case ParameterStyle.EscapedInline =>
        if d.canExtractDateTimeComponentsNatively then
          ctx.sql.append(
            "TIMESTAMP'" + ldt.format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) + "'"
          )
        else
          ctx.sql.append("'" + ldt.format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) + "'")
      case ParameterStyle.DriverParametrized =>
        ctx.sql.append(preferredPlaceholder(ctx, ctx.parameters.size + 1))
        ctx.parameters.append(ldt.asInstanceOf[Object])

case class LiteralInteger(number: Long, ast: Expr[?, ?]) extends QueryIRLeaf:
  override val precedence: Int = Precedence.Literal
  override def computeSQL(using d: Dialect)(using cnf: Config)(ctx: SQLRenderingContext): Unit =
    cnf.parameterStyle match
      case ParameterStyle.EscapedInline =>
        ctx.sql.append(number.toString)
      case ParameterStyle.DriverParametrized =>
        ctx.sql.append(preferredPlaceholder(ctx, ctx.parameters.size + 1))
        ctx.parameters.append(number.asInstanceOf[Object])

case class LiteralDouble(number: Double, ast: Expr[?, ?]) extends QueryIRLeaf:
  override val precedence: Int = Precedence.Literal
  override def computeSQL(using d: Dialect)(using cnf: Config)(ctx: SQLRenderingContext): Unit =
    cnf.parameterStyle match
      case ParameterStyle.EscapedInline =>
        ctx.sql.append(number.toString)
      case ParameterStyle.DriverParametrized =>
        ctx.sql.append(preferredPlaceholder(ctx, ctx.parameters.size + 1))
        ctx.parameters.append(number.asInstanceOf[Object])

/** List expression, for DBs that support lists/arrays.
  */
case class ListTypeExpr(elements: List[QueryIRNode], ast: Expr[?, ?]) extends QueryIRNode:
  override val precedence: Int = Precedence.Literal
  override def computeSQL(using d: Dialect)(using cnf: Config)(ctx: SQLRenderingContext): Unit =
    ctx.mkString(
      elements,
      "(ARRAY[",
      ", ",
      "])"
    ) // ()s are needed in Postgres for indexing, ARRAY[1,2,3][1] will not work there

/** Empty leaf node, to avoid Options everywhere.
  */
case class EmptyLeaf(ast: DatabaseAST[?] = null) extends QueryIRLeaf:
  override def computeSQL(using d: Dialect)(using cnf: Config)(ctx: SQLRenderingContext): Unit = ()

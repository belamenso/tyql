package tyql

import language.experimental.namedTuples
import NamedTuple.{AnyNamedTuple, NamedTuple}
import scala.annotation.{implicitNotFound, targetName}
import scala.reflect.ClassTag
import Utils.{HasDuplicate, naturalMap}
import tyql.Expr.StripExpr
import tyql.RestrictedQuery.ToRestrictedQueryRef
import scala.deriving.Mirror
import scala.compiletime.constValueTuple

sealed trait ResultCategory
class SetResult extends ResultCategory
class BagResult extends ResultCategory

trait Query[A, Category <: ResultCategory](using ResultTag[A]) extends DatabaseAST[A]:
  import Expr.{Fun, Ref}
  val tag: ResultTag[A] = qTag

  // XXX currently ugly and cannot be abstracted due to the `copy` method existing only in case classes
  protected def copyWith
    (requestedJoinType: Option[JoinType], requestedJoinOn: Option[Fun[A, Expr[Boolean, NonScalarExpr], NonScalarExpr]])
    : Query[A, Category]

  var requestedJoinType: Option[JoinType] = None
  var requestedJoinOn: Option[Fun[?, Expr[Boolean, NonScalarExpr], NonScalarExpr]] = None

  def joinOn(f: Ref[A, NonScalarExpr] => Expr[Boolean, NonScalarExpr]): Query[A, Category] =
    val ref = Ref[A, NonScalarExpr]()
    val ret = copyWith(Some(JoinType.InnerExplicit), Some(Fun(ref, f(ref))))
    ret

  def leftJoinOn(f: Ref[A, NonScalarExpr] => Expr[Boolean, NonScalarExpr]): Query[A, Category] =
    val ref = Ref[A, NonScalarExpr]()
    val ret = copyWith(Some(JoinType.LeftOuter), Some(Fun(ref, f(ref))))
    ret

  def rightJoinOn(f: Ref[A, NonScalarExpr] => Expr[Boolean, NonScalarExpr]): Query[A, Category] =
    val ref = Ref[A, NonScalarExpr]()
    val ret = copyWith(Some(JoinType.RightOuter), Some(Fun(ref, f(ref))))
    ret

  def fullOuterJoinOn(f: Ref[A, NonScalarExpr] => Expr[Boolean, NonScalarExpr]): Query[A, Category] =
    val ref = Ref[A, NonScalarExpr]()
    val ret = copyWith(Some(JoinType.FullOuter), Some(Fun(ref, f(ref))))
    ret

  /** Classic flatMap with an inner Query that will likely be flattened into a join.
    *
    * @param f
    *   a function that returns a Query (so no aggregations in the subtree)
    * @tparam B
    *   the result type of the query.
    * @return
    *   Query[B], e.g. an iterable of results of type B
    */
  def flatMap[B: ResultTag](f: Ref[A, NonScalarExpr] => Query[B, ?]): Query[B, BagResult] =
    val ref = Ref[A, NonScalarExpr]()
    val r = Query.FlatMap(this, Fun(ref, f(ref)))
    r.requestedJoinOn = requestedJoinOn
    r.requestedJoinType = requestedJoinType
    r

  /** Classic flatMap with an inner query that is a RestrictedQuery. This turns the result query into a RestrictedQuery.
    * Exists to support doing BaseCaseRelation.flatMap(...) within a fix
    * @param f
    *   a function that returns a RestrictedQuery, meaning it has used a recursive definition from fix.
    * @tparam B
    *   the result type of the query.
    * @return
    *   RestrictedQuery[B]
    */
  def flatMap[B: ResultTag, D <: Tuple]
    (f: Ref[A, NonScalarExpr] => RestrictedQuery[B, ?, D])
    : RestrictedQuery[B, BagResult, D] =
    val ref = Ref[A, NonScalarExpr]()
    val r = Query.FlatMap(this, Fun(ref, f(ref).toQuery))
    r.requestedJoinOn = requestedJoinOn
    r.requestedJoinType = requestedJoinType
    RestrictedQuery(r)

  /** Equivalent to flatMap(f: Ref => Aggregation). NOTE: make Ref of type NExpr so that relation.id is counted as
    * NExpr, not ScalarExpr
    *
    * @param f
    *   a function that returns an Aggregation (guaranteed agg in subtree)
    * @tparam B
    *   the result type of the aggregation.
    * @return
    *   Aggregation[B], a scalar result, e.g. a single value of type B.
    */
  def aggregate[B: ResultTag, T <: Tuple](f: Ref[A, NonScalarExpr] => Aggregation[T, B]): Aggregation[A *: T, B] =
    val ref = Ref[A, NonScalarExpr]()
    Aggregation.AggFlatMap[A *: T, B](this, Fun(ref, f(ref)))

  /** Equivalent version of map(f: Ref => AggregationExpr). Requires f to call toRow on the final result before
    * returning. Sometimes the implicit conversion kicks in and converts a named-tuple-of-Agg into a Agg-of-named-tuple,
    * but not always. As an alternative, can use the aggregate defined below that explicitly calls toRow on the result
    * of f.
    *
    * @param f
    *   a function that returns an Aggregation (guaranteed agg in subtree)
    * @tparam B
    *   the result type of the aggregation.
    * @return
    *   Aggregation[B], a scalar result, e.g. single value of type B.
    */
  @targetName("AggregateExpr")
  def aggregate[B: ResultTag](f: Ref[A, NonScalarExpr] => AggregationExpr[B]): Aggregation[A *: EmptyTuple, B] =
    val ref = Ref[A, NonScalarExpr]()
    Aggregation.AggFlatMap[A *: EmptyTuple, B](this, Fun(ref, f(ref)))

  /** A version of the above-defined aggregate that allows users to skip calling toRow on the result in f.
    *
    * @param f
    *   a function that returns a named-tuple-of-Aggregation.
    * @tparam B
    *   the named-tuple-of-Aggregation that will be converted to an Aggregation-of-named-tuple
    * @return
    *   Aggregation of B.toRow, e.g. a scalar result of type B.toRow
    */
  def aggregate[
      B <: AnyNamedTuple : AggregationExpr.IsTupleOfAgg
  ] /*(using ev: AggregationExpr.IsTupleOfAgg[B] =:= true)*/
    (using ResultTag[NamedTuple.Map[B, Expr.StripExpr]])
    (f: Ref[A, NonScalarExpr] => B)
    : Aggregation[A *: EmptyTuple, NamedTuple.Map[B, Expr.StripExpr]] =

    import AggregationExpr.toRow
    val ref = Ref[A, NonScalarExpr]()
    val row = f(ref).toRow
    Aggregation.AggFlatMap[A *: EmptyTuple, NamedTuple.Map[B, Expr.StripExpr]](
      this,
      Fun(ref, row.asInstanceOf[Expr[NamedTuple.Map[B, Expr.StripExpr], ScalarExpr]])
    )

  // inline def aggregate[B: ResultTag](f: Ref[A, ScalarExpr] => Query[B]): Nothing =
  // error("No aggregation function found in f. Did you mean to use flatMap?")

// TODO Restrictions for groupBy: all columns in the selectFn must either be in the groupingFn or in an aggregate.
//      This construction leads to "can't prove" from the compiler.
//      match on named tuple must be before match on AggrExpr/Expr, otherwise the compiler complains that it cannot prove that a named tuple is disjoint from an expression
  // type GetFields[T] <: Tuple = T match
  //   case NamedTuple[n, v] => n
  //   case Expr[t, ?] => GetFields[t]
  //   case _ => EmptyTuple
  // type GetFieldsWithoutAggregates[T] <: Tuple = T match
  //   case NamedTuple[n, v] => n
  //   case AggregationExpr[t] => EmptyTuple
  //   case Expr[t, ?] => GetFields[t]
  //   case _ => EmptyTuple

// TODO figure out how to do groupBy on join result.
//      GroupBy grouping function when applied to the result of a join accesses only columns from the original queries.
// XXX `groupBy`, `groupByAggregate`, and `filterByGroupBy` are now separated due to issues with overloading, but could be condensed into a single groupBy method
  /** groupBy where the grouping clause is NOT an aggregation. Can add a 'having' statement incrementally by calling
    * .having on the result. The selectFn MUST return an aggregation.
    *
    * @param groupingFn
    *   \- must be a named tuple, in order from left->right. Must return an non-scalar expression.
    * @param selectFn
    *   \- the project clause of the select statement that is grouped NOTE: filterFn - the HAVING clause is used to
    *   filter groups after the GROUP BY operation has been applied. filters applied before the groupBy occur in the
    *   WHERE clause.
    * @tparam R
    *   \- the return type of the expression
    * @tparam GroupResult
    *   \- the type of the grouping statement
    * @return
    */
  def groupBy[R: ResultTag, GroupResult]
    (
        groupingFn: Ref[A, NonScalarExpr] => Expr[GroupResult, NonScalarExpr],
        selectFn: Ref[A, ScalarExpr] => Expr[R, ScalarExpr]
    )
  // (using ev: Tuple.Union[GetFieldsWithoutAggregates[R]] <:< Tuple.Union[GetFields[GroupResult]])
    : Query.GroupBy[A, R, GroupResult, NonScalarExpr, ScalarExpr] =
    val refG = Ref[A, NonScalarExpr]()
    val groupFun = Fun(refG, groupingFn(refG))

    val refS = Ref[A, ScalarExpr]()
    val selectFun = Fun(refS, selectFn(refS))
    Query.GroupBy(this, groupFun, selectFun, None)

  /** groupBy where the grouping clause IS an aggregation. Can add a 'having' statement incrementally by calling .having
    * on the result. The selectFn MUST return an aggregation.
    *
    * @param groupingFn
    *   \- must be a named tuple, in order from left->right. Must return an aggregation.
    * @param selectFn
    *   \- the project clause of the select statement that is grouped. NOTE: filterFn - the HAVING clause is used to
    *   filter groups after the GROUP BY operation has been applied. filters applied before the groupBy occur in the
    *   WHERE clause.
    * @tparam R
    *   \- the return type of the expression
    * @tparam GroupResult
    *   \- the type of the grouping statement
    * @return
    */
  def groupByAggregate[R: ResultTag, GroupResult]
    (
        groupingFn: Ref[A, ScalarExpr] => Expr[GroupResult, ScalarExpr],
        selectFn: Ref[A, ScalarExpr] => Expr[R, ScalarExpr]
    )
    : Query.GroupBy[A, R, GroupResult, ScalarExpr, ScalarExpr] =
    val refG = Ref[A, ScalarExpr]()
    val groupFun = Fun(refG, groupingFn(refG))

    val refS = Ref[A, ScalarExpr]()
    val selectFun = Fun(refS, selectFn(refS))
    Query.GroupBy(this, groupFun, selectFun, None)

  /** filter based on a groupBy result. The selectFn MUST NOT return an aggregation. The groupingFn MUST NOT return an
    * aggregation. The filterFn MUST return an aggregation.
    *
    * @param groupingFn
    *   \- must be a named tuple, in order from left->right. Must return a scalar expression.
    * @param selectFn
    *   \- the project clause of the select statement that is grouped. Must return a scalar expression.
    * @param havingFn
    *   \- the filter clause. Must return an aggregation.
    * @tparam R
    *   \- the return type of the expression
    * @tparam GroupResult
    *   \- the type of the grouping statement
    * @return
    */
  def filterByGroupBy[R: ResultTag, GroupResult]
    (
        groupingFn: Ref[A, NonScalarExpr] => Expr[GroupResult, NonScalarExpr],
        selectFn: Ref[A, NonScalarExpr] => Expr[R, NonScalarExpr],
        havingFn: Ref[A, ?] => Expr[Boolean, ?]
    )
    : Query.GroupBy[A, R, GroupResult, NonScalarExpr, NonScalarExpr] =
    val refG = Ref[A, NonScalarExpr]()
    val groupFun = Fun(refG, groupingFn(refG))

    val refS = Ref[A, NonScalarExpr]()
    val selectFun = Fun(refS, selectFn(refS))

    // Cast is workaround for: "ScalarExpr is not subtype of ?"
    Query.GroupBy(this, groupFun, selectFun, None).having(havingFn).asInstanceOf[Query.GroupBy[
      A,
      R,
      GroupResult,
      NonScalarExpr,
      NonScalarExpr
    ]]

//  def groupByAny[R: ResultTag, GroupResult, GroupShape <: ExprShape](
//    groupingFn: Ref[A, GroupShape] => Expr[GroupResult, GroupShape],
//    selectFn: Ref[A, ScalarExpr] => Expr[R, ScalarExpr]
//  ): Query.GroupBy[A, R, GroupResult, GroupShape] =
//    val refG = Ref[A, GroupShape]()
//    val groupFun = Fun(refG, groupingFn(refG))
//
//    val refS = Ref[A, ScalarExpr]()
//    val selectFun = Fun(refS, selectFn(refS))
//    Query.GroupBy(this, groupFun, selectFun, None)

  /** Classic map with an inner expression to transform the row. Requires f to call toRow on the final result before
    * returning. Sometimes the implicit conversion kicks in and converts a named-tuple-of-Expr into a
    * Expr-of-named-tuple, but not always. As an alternative, can use the map defined below that explicitly calls toRow
    * on the result of f.
    *
    * @param f
    *   a function that returns a Expression.
    * @tparam B
    *   the result type of the Expression, and resulting query.
    * @return
    *   Query[B], an iterable of type B.
    */
  def map[B: ResultTag](f: Ref[A, NonScalarExpr] => Expr[B, NonScalarExpr]): Query[B, BagResult] =
    val ref = Ref[A, NonScalarExpr]()
    val r = Query.Map(this, Fun(ref, f(ref)))
    r.requestedJoinOn = requestedJoinOn
    r.requestedJoinType = requestedJoinType
    r

  /** A version of the above-defined map that allows users to skip calling toRow on the result in f.
    *
    * @param f
    *   a function that returns a named-tuple-of-Expr.
    * @tparam B
    *   the named-tuple-of-Expr that will be converted to an Expr-of-named-tuple
    * @return
    *   Expr of B.toRow, e.g. an iterable of type B.toRow
    */
  def map[B <: AnyNamedTuple : Expr.IsTupleOfExpr]
    (using ResultTag[NamedTuple.Map[B, Expr.StripExpr]])
    (f: Ref[A, NonScalarExpr] => B)
    : Query[NamedTuple.Map[B, Expr.StripExpr], BagResult] =
    import Expr.toRow
    val ref = Ref[A, NonScalarExpr]()
    val r = Query.Map(this, Fun(ref, f(ref).toRow))
    r.requestedJoinOn = requestedJoinOn
    r.requestedJoinType = requestedJoinType
    r

  def fix(p: RestrictedQueryRef[A, ?, 0] => RestrictedQuery[A, SetResult, Tuple1[0]]): Query[A, SetResult] =
    type QT = Tuple1[Query[A, ?]]
    type DT = Tuple1[Tuple1[0]]
    type RQT = Tuple1[RestrictedQuery[A, SetResult, Tuple1[0]]]
    val fn: Tuple1[RestrictedQueryRef[A, ?, 0]] => RQT = r => Tuple1(p(r._1))
    Query.fix[QT, DT, RQT](Tuple1(this))(fn)._1

  def unrestrictedFix(p: Query.QueryRef[A, ?] => Query[A, ?]): Query[A, BagResult] =
    type QT = Tuple1[Query[A, ?]]
    val fn: Tuple1[Query.QueryRef[A, ?]] => Tuple1[Query[A, ?]] = r => Tuple1(p(r._1))
    val t = Query.unrestrictedFix[QT](Tuple1(this))(fn)._1
    t.asInstanceOf[Query[A, BagResult]]

  def unrestrictedBagFix(p: Query.QueryRef[A, ?] => Query[A, ?]): Query[A, BagResult] =
    type QT = Tuple1[Query[A, ?]]
    val fn: Tuple1[Query.QueryRef[A, ?]] => Tuple1[Query[A, ?]] = r => Tuple1(p(r._1))
    val t = Query.unrestrictedBagFix[QT](Tuple1(this))(fn)._1
    t.asInstanceOf[Query[A, BagResult]]

  def withFilter(p: Ref[A, NonScalarExpr] => Expr[Boolean, NonScalarExpr]): Query[A, Category] =
    val ref = Ref[A, NonScalarExpr]()
    Query.Filter(this, Fun(ref, p(ref)))

  def filter(p: Ref[A, NonScalarExpr] => Expr[Boolean, NonScalarExpr]): Query[A, Category] = withFilter(p)

  def nonEmpty: Expr[Boolean, NonScalarExpr] =
    Expr.NonEmpty(this)

  def isEmpty: Expr[Boolean, NonScalarExpr] =
    Expr.IsEmpty(this)

  /** When there is only one relation to be defined recursively.
    */
  def sort[B](f: Ref[A, NonScalarExpr] => Expr[B, NonScalarExpr], ord: Ord = Ord.ASC): Query[A, Category] =
    val ref = Ref[A, NonScalarExpr]()
    Query.Sort(this, Fun(ref, f(ref)), ord)

  def sortDesc[B](f: Ref[A, NonScalarExpr] => Expr[B, NonScalarExpr]): Query[A, Category] = sort[B](f, Ord.DESC)

  // offset and not drop since _.take.drop and _.drop.take are not equivalent like in SQL
  def take(lim: Int): Query[A, Category] = Query.Limit(this, lim)
  def limit(lim: Int): Query[A, Category] = take(lim)
  def offset(lim: Int): Query[A, Category] = Query.Offset(this, lim)

  def distinct: Query[A, SetResult] = Query.Distinct(this)

  def sum[B: ResultTag](f: Ref[A, NonScalarExpr] => Expr[B, NonScalarExpr]): Aggregation[A *: EmptyTuple, B] =
    val ref = Ref[A, NonScalarExpr]()
    Aggregation.AggFlatMap(this, Fun(ref, AggregationExpr.Sum(f(ref))))

  def avg[B: ResultTag](f: Ref[A, NonScalarExpr] => Expr[B, NonScalarExpr]): Aggregation[A *: EmptyTuple, B] =
    val ref = Ref[A, NonScalarExpr]()
    Aggregation.AggFlatMap(this, Fun(ref, AggregationExpr.Avg(f(ref))))

  def max[B: ResultTag](f: Ref[A, NonScalarExpr] => Expr[B, NonScalarExpr]): Aggregation[A *: EmptyTuple, B] =
    val ref = Ref[A, NonScalarExpr]()
    Aggregation.AggFlatMap(this, Fun(ref, AggregationExpr.Max(f(ref))))

  def min[B: ResultTag](f: Ref[A, NonScalarExpr] => Expr[B, NonScalarExpr]): Aggregation[A *: EmptyTuple, B] =
    val ref = Ref[A, NonScalarExpr]()
    Aggregation.AggFlatMap(this, Fun(ref, AggregationExpr.Min(f(ref))))

  def size: Aggregation[A *: EmptyTuple, Int] =
    val ref = Ref[A, ScalarExpr]()
    Aggregation.AggFlatMap(this, Fun(ref, AggregationExpr.Count(Expr.IntLit(1))))

  def union(that: Query[A, ?]): Query[A, SetResult] =
    Query.Union(this, that)

  def unionAll(that: Query[A, ?]): Query[A, BagResult] =
    Query.UnionAll(this, that)

  def intersect(that: Query[A, ?]): Query[A, SetResult] =
    Query.Intersect(this, that)

  def intersectAll(that: Query[A, ?]): Query[A, BagResult] =
    Query.IntersectAll(this, that)

  def except(that: Query[A, ?]): Query[A, SetResult] =
    Query.Except(this, that)

  def exceptAll(that: Query[A, ?]): Query[A, BagResult] =
    Query.ExceptAll(this, that)

  // XXX we are waiting for https://github.com/scala/scala3/issues/22392
  inline def insertInto[R, PartialNames <: Tuple]
    (table: InsertableTable[R, PartialNames])
    (using
        @implicitNotFound(
          "The column names you insert must be the same you insert into. If needed, use table.partial to select the names you insert into. You are selecting into ${PartialNames}."
        )
        ev0: TypeOperations.IsSubset[PartialNames, NamedTuple.Names[NamedTuple.From[A]]],
        @implicitNotFound(
          "The column names you insert must be the same you insert into. If needed, use table.partial to select the names you insert into. You are selecting into ${PartialNames}."
        )
        ev1: TypeOperations.IsSubset[NamedTuple.Names[NamedTuple.From[A]], PartialNames],
        @implicitNotFound(
          "The types you insert into must be acceptable for their target types. E.g. you can insert Int into a Long or Option[Int] or Int."
        )
        ev2: TypeOperations.IsAcceptableInsertion[
          Tuple.Map[
            TypeOperations.SelectByNames[
              PartialNames,
              NamedTuple.DropNames[NamedTuple.From[A]],
              NamedTuple.Names[NamedTuple.From[A]]
            ],
            Expr.StripExpr
          ],
          TypeOperations.SelectByNames[
            PartialNames,
            NamedTuple.DropNames[NamedTuple.From[A]],
            NamedTuple.Names[NamedTuple.From[A]]
          ]
        ] =:= true
    )
    : InsertFromSelect[R, A] = InsertFromSelect(
    table.underlyingTable,
    this,
    constValueTuple[NamedTuple.Names[NamedTuple.From[A]]].toList.asInstanceOf[List[String]]
  )

end Query // trait

// XXX Due to bugs in Scala compiler, if you replace `: ResultTag` with `using ...` then it will not compile
extension [A: ResultTag, Category <: ResultCategory](using DialectFeature.INCanHandleRows)(q: Query[A, Category])
  def containsRow(expr: Expr[A, NonScalarExpr]): Expr[Boolean, NonScalarExpr] =
    Expr.Contains(q, expr)

extension [A: ResultTag, Category <: ResultCategory]
  (q: Query[A, Category])
  (using
      @implicitNotFound(
        "You can only call contains on queries that return a simple list of values, like Double. You might want to map your result like this: q.map(row => row.price)"
      ) ev: SimpleTypeResultTag[A]
  )
  def contains(expr: Expr[A, NonScalarExpr]): Expr[Boolean, NonScalarExpr] =
    Expr.Contains(q, expr)

// type IsTupleOfExpr[A <: AnyNamedTuple] = Tuple.Union[NamedTuple.DropNames[A]] <:< Expr[?, ?]
inline def Values[A <: AnyNamedTuple]
  (v: NamedTuple.DropNames[A]*)
  (using ResultTag[NamedTuple[NamedTuple.Names[A], NamedTuple.DropNames[A]]])
  : Query[NamedTuple[NamedTuple.Names[A], NamedTuple.DropNames[A]], BagResult] =
  ValuesInner[NamedTuple.Names[A], NamedTuple.DropNames[A]](v.map(z => z.withNames[NamedTuple.Names[A]]))
private inline def ValuesInner[N <: Tuple, T <: Tuple]
  (v: Seq[NamedTuple[N, T]])
  (using ResultTag[NamedTuple[N, T]])
  : Query[NamedTuple[N, T], BagResult] =
  Query.ValuesQuery(v.map(z => z.asInstanceOf[Tuple]), constValueTuple[N].toList.asInstanceOf[List[String]])

type ListOfExprsOrRaws[T <: Tuple] = T match
  case EmptyTuple => EmptyTuple
  case t *: ts    => (Expr[t, NonScalarExpr] | t) *: ListOfExprsOrRaws[ts]

inline def Exprs[A <: AnyNamedTuple]
  (v: ListOfExprsOrRaws[NamedTuple.DropNames[A]])
  (using ResultTag[A])
  : Query[A, BagResult] =
  Query.ExprsQuery[A](
    v.asInstanceOf[Tuple].toList,
    constValueTuple[NamedTuple.Names[A]].toList.asInstanceOf[List[String]]
  )

object Query:
  import Expr.{Pred, Fun, Ref}
  import RestrictedQuery.*

  def monotoneFix[QT <: Tuple]
    (bases: QT)
    (using Tuple.Union[QT] <:< Query[?, ?])
    (fns: ToMonotoneQueryRef[QT] => ToMonotoneQuery[QT])
    : ToQuery[QT] =
    fixImpl(bases)(fns)

  // No restrictions at all but still use set semantics (union between base + recur)
  def unrestrictedFix[QT <: Tuple]
    (bases: QT)
    (using Tuple.Union[QT] <:< Query[?, ?])
    (fns: ToUnrestrictedQueryRef[QT] => ToUnrestrictedQuery[QT])
    : ToQuery[QT] =
    unrestrictedFixImpl(true)(bases)(fns)

  // No restrictions at all, use bag semantics
  def unrestrictedBagFix[QT <: Tuple]
    (bases: QT)
    (using Tuple.Union[QT] <:< Query[?, ?])
    (fns: ToUnrestrictedQueryRef[QT] => ToUnrestrictedQuery[QT])
    : ToQuery[QT] =
    unrestrictedFixImpl(false)(bases)(fns)

  /** Fixed point computation.
    *
    * @param bases
    *   Tuple of the base case queries. NOTE: bases do not have to be sets since they will be directly fed into an
    *   union. TODO: in theory, the results could be assumed to be sets due to the outermost UNION(base, recur), but
    *   better to enforce .distinct (also bc we flatten the unions)
    * @param fns
    *   A function from a tuple of restricted query refs to a tuple of *SET* restricted queries.
    *
    * QT is the tuple of Query DT is the tuple of tuple of constant ints. Used to track dependencies of recursive
    * definitions in order to enforce that every recursive reference is used at least once. RQT is the tuple of
    * RestrictedQuery that should be returned from fns.
    *
    * NOTE: Because groupBy is a Query not an Aggregation, the results of groupBy are allowed as base-cases of recursive
    * queries. This is consistent with stratified aggregation. However, if you wanted to prevent recursion of any kind
    * within mutual recursive queries (e.g. postgres), would need to add a constraint to prevent fix(...) from taking in
    * anything of type GroupBy within QT. Not applicable to inline fix, since the restriction is only for mutual
    * recursion.
    */
  def fix[QT <: Tuple, DT <: Tuple, RQT <: Tuple]
    (bases: QT)
    (fns: ToRestrictedQueryRef[QT] => RQT)
    (using
        @implicitNotFound(
          "Number of base cases must match the number of recursive definitions returned by fns"
        ) ev0: Tuple.Size[QT] =:= Tuple.Size[RQT]
    )
    (using @implicitNotFound("Base cases must be of type Query: ${QT}") ev1: Union[QT] <:< Query[?, ?])
    (using @implicitNotFound("Cannot constrain DT") ev2: DT <:< InverseMapDeps[RQT])
    (using @implicitNotFound("Recursive definitions must be linear: ${RQT}") ev3: RQT <:< ToRestrictedQuery[QT, DT])
    (using
        @implicitNotFound(
          "Recursive definitions must be linear, e.g. recursive references must appear at least once in all the recursive definitions: ${RQT}"
        ) ev4: ExpectedResult[QT] <:< ActualResult[RQT]
    )
//                                    (using @implicitNotFound("Recursive definitions must be linear, e.g. recursive references cannot appear twice within the same recursive definition: ${RQT}") ev2: Tuple.Union[Tuple.Map[DT, CheckDuplicate]] =:= false)
    : ToQuery[QT] =
    fixImpl(bases)(fns)

  def unrestrictedFixImpl[QT <: Tuple, P <: Tuple, R <: Tuple](setBased: Boolean)(bases: QT)(fns: P => R): ToQuery[QT] =
    // If base cases are themselves recursive definitions.
    val baseRefsAndDefs = bases.toArray.map {
      case MultiRecursive(params, querys, resultQ) =>
        ??? // TODO: decide on semantics for multiple fix definitions. (param, query)
      case base: Query[?, ?] => (RestrictedQueryRef()(using base.tag), base)
    }
    val refsTuple = Tuple.fromArray(baseRefsAndDefs.map(_._1)).asInstanceOf[P]
    val unrestrictedRefsTuple = Tuple.fromArray(baseRefsAndDefs.map(_._1.toQueryRef)).asInstanceOf[P]
    val refsList = baseRefsAndDefs.map(_._1).toList
    val recurQueries = fns(unrestrictedRefsTuple)

    val baseCaseDefsList = baseRefsAndDefs.map(_._2.asInstanceOf[Query[?, ?]])
    val recursiveDefsList: List[Query[?, ?]] =
      if (setBased)
        recurQueries.toList.map(_.asInstanceOf[Query[?, ?]]).lazyZip(baseCaseDefsList).map:
          case (query: Query[t, c], ddef) =>
            // Optimization: remove any extra .distinct calls that are getting fed into a union anyway
            val lhs = ddef match
              case Distinct(from) => from
              case t              => t
            val rhs = query match
              case Distinct(from) => from
              case t              => t
            Union(lhs.asInstanceOf[Query[t, c]], rhs.asInstanceOf[Query[t, c]])(using query.tag)
      else
        recurQueries.toList.map(_.asInstanceOf[Query[?, ?]]).lazyZip(baseCaseDefsList).map:
          case (query: Query[t, c], ddef) =>
            UnionAll(ddef.asInstanceOf[Query[t, c]], query.asInstanceOf[Query[t, c]])(using query.tag)

    val rt = refsTuple.asInstanceOf[Tuple.Map[Elems[QT], [T] =>> RestrictedQueryRef[T, ?, ?]]]
    rt.naturalMap([t] =>
      finalRef =>
        val fr = finalRef.asInstanceOf[RestrictedQueryRef[t, ?, ?]]

        given ResultTag[t] = fr.tag

        MultiRecursive(
          refsList,
          recursiveDefsList,
          fr.toQueryRef
      )
    )

  def fixImpl[QT <: Tuple, P <: Tuple, R <: Tuple](bases: QT)(fns: P => R): ToQuery[QT] =
    // If base cases are themselves recursive definitions.
    val baseRefsAndDefs = bases.toArray.map {
      case MultiRecursive(params, querys, resultQ) =>
        ??? // TODO: decide on semantics for multiple fix definitions. (param, query)
      case base: Query[?, ?] => (RestrictedQueryRef()(using base.tag), base)
    }
    val refsTuple = Tuple.fromArray(baseRefsAndDefs.map(_._1)).asInstanceOf[P]
    val refsList = baseRefsAndDefs.map(_._1).toList
    val recurQueries = fns(refsTuple)

    val baseCaseDefsList = baseRefsAndDefs.map(_._2.asInstanceOf[Query[?, ?]])
    val recursiveDefsList: List[Query[?, ?]] =
      recurQueries.toList.map(_.asInstanceOf[RestrictedQuery[?, ?, ?]].toQuery).lazyZip(baseCaseDefsList).map:
        case (query: Query[t, c], ddef) =>
          // Optimization: remove any extra .distinct calls that are getting fed into a union anyway
          val lhs = ddef match
            case Distinct(from) => from
            case t              => t
          val rhs = query match
            case Distinct(from) => from
            case t              => t
          Union(lhs.asInstanceOf[Query[t, c]], rhs.asInstanceOf[Query[t, c]])(using query.tag)

    val rt = refsTuple.asInstanceOf[Tuple.Map[Elems[QT], [T] =>> RestrictedQueryRef[T, ?, ?]]]
    rt.naturalMap([t] =>
      finalRef =>
        val fr = finalRef.asInstanceOf[RestrictedQueryRef[t, ?, ?]]
        given ResultTag[t] = fr.tag
        MultiRecursive(
          refsList,
          recursiveDefsList,
          fr.toQueryRef
      )
    )

  // TODO: in the case we want to allow bag semantics within recursive queries, set $bag
  case class MultiRecursive[R]
    ($param: List[RestrictedQueryRef[?, ?, ?]], $subquery: List[Query[?, ?]], $resultQuery: Query[R, ?])
    (using ResultTag[R]) extends Query[R, SetResult] {
    override def copyWith
      (
          requestedJoinType: Option[JoinType],
          requestedJoinOn: Option[Fun[R, Expr[Boolean, NonScalarExpr], NonScalarExpr]]
      )
      : Query[R, SetResult] =
      val ret = this.copy()
      ret.requestedJoinOn = requestedJoinOn
      ret.requestedJoinType = requestedJoinType
      ret
  }

  // XXX currently queries share this counter, which might result in larger numbers over time, but should not be dangerous since these are longs
  private var refCount = 0L
  case class QueryRef[A: ResultTag, C <: ResultCategory]() extends Query[A, C]:
    private val id = refCount
    refCount += 1
    def stringRef() = s"recref$id"
    override def toString: String = s"QueryRef[${stringRef()}]"
    override def copyWith
      (
          requestedJoinType: Option[JoinType],
          requestedJoinOn: Option[Fun[A, Expr[Boolean, NonScalarExpr], NonScalarExpr]]
      )
      : Query[A, C] =
      val ret = this.copy()
      ret.requestedJoinOn = requestedJoinOn
      ret.requestedJoinType = requestedJoinType
      ret.asInstanceOf[Query[A, C]]

  case class Filter[A: ResultTag, C <: ResultCategory]($from: Query[A, C], $pred: Pred[A, NonScalarExpr])
      extends Query[A, C] {
    override def copyWith
      (
          requestedJoinType: Option[JoinType],
          requestedJoinOn: Option[Fun[A, Expr[Boolean, NonScalarExpr], NonScalarExpr]]
      )
      : Query[A, C] =
      val ret = this.copy()
      ret.requestedJoinOn = requestedJoinOn
      ret.requestedJoinType = requestedJoinType
      ret
  }
  case class Map[A, B: ResultTag]($from: Query[A, ?], $query: Fun[A, Expr[B, ?], ?]) extends Query[B, BagResult] {
    override def copyWith
      (
          requestedJoinType: Option[JoinType],
          requestedJoinOn: Option[Fun[B, Expr[Boolean, NonScalarExpr], NonScalarExpr]]
      )
      : Query[B, BagResult] =
      val ret = this.copy()
      ret.requestedJoinOn = requestedJoinOn
      ret.requestedJoinType = requestedJoinType
      ret
  }
  case class FlatMap[A, B: ResultTag]($from: Query[A, ?], $query: Fun[A, Query[B, ?], NonScalarExpr])
      extends Query[B, BagResult] {
    override def copyWith
      (
          requestedJoinType: Option[JoinType],
          requestedJoinOn: Option[Fun[B, Expr[Boolean, NonScalarExpr], NonScalarExpr]]
      )
      : Query[B, BagResult] =
      val ret = this.copy()
      ret.requestedJoinOn = requestedJoinOn
      ret.requestedJoinType = requestedJoinType
      ret
  }
  // case class Sort[A]($q: Query[A], $o: Ordering[A]) extends Query[A] // alternative syntax to avoid chaining .sort for multi-key sort
  case class Sort[A: ResultTag, B, C <: ResultCategory]
    ($from: Query[A, C], $body: Fun[A, Expr[B, NonScalarExpr], NonScalarExpr], $ord: Ord) extends Query[A, C] {
    override def copyWith
      (
          requestedJoinType: Option[JoinType],
          requestedJoinOn: Option[Fun[A, Expr[Boolean, NonScalarExpr], NonScalarExpr]]
      )
      : Query[A, C] =
      val ret = this.copy()
      ret.requestedJoinOn = requestedJoinOn
      ret.requestedJoinType = requestedJoinType
      ret
  }
  case class Limit[A: ResultTag, C <: ResultCategory]($from: Query[A, C], $limit: Int) extends Query[A, C] {
    override def copyWith
      (
          requestedJoinType: Option[JoinType],
          requestedJoinOn: Option[Fun[A, Expr[Boolean, NonScalarExpr], NonScalarExpr]]
      )
      : Query[A, C] =
      val ret = this.copy()
      ret.requestedJoinOn = requestedJoinOn
      ret.requestedJoinType = requestedJoinType
      ret
  }
  case class Offset[A: ResultTag, C <: ResultCategory]($from: Query[A, C], $offset: Int) extends Query[A, C] {
    override def copyWith
      (
          requestedJoinType: Option[JoinType],
          requestedJoinOn: Option[Fun[A, Expr[Boolean, NonScalarExpr], NonScalarExpr]]
      )
      : Query[A, C] =
      val ret = this.copy()
      ret.requestedJoinOn = requestedJoinOn
      ret.requestedJoinType = requestedJoinType
      ret
  }
  case class Distinct[A: ResultTag]($from: Query[A, ?]) extends Query[A, SetResult] {
    override def copyWith
      (
          requestedJoinType: Option[JoinType],
          requestedJoinOn: Option[Fun[A, Expr[Boolean, NonScalarExpr], NonScalarExpr]]
      )
      : Query[A, SetResult] =
      val ret = this.copy()
      ret.requestedJoinOn = requestedJoinOn
      ret.requestedJoinType = requestedJoinType
      ret
  }

  case class Union[A: ResultTag]($this: Query[A, ?], $other: Query[A, ?]) extends Query[A, SetResult] {
    override def copyWith
      (
          requestedJoinType: Option[JoinType],
          requestedJoinOn: Option[Fun[A, Expr[Boolean, NonScalarExpr], NonScalarExpr]]
      )
      : Query[A, SetResult] =
      val ret = this.copy()
      ret.requestedJoinOn = requestedJoinOn
      ret.requestedJoinType = requestedJoinType
      ret
  }
  case class UnionAll[A: ResultTag]($this: Query[A, ?], $other: Query[A, ?]) extends Query[A, BagResult] {
    override def copyWith
      (
          requestedJoinType: Option[JoinType],
          requestedJoinOn: Option[Fun[A, Expr[Boolean, NonScalarExpr], NonScalarExpr]]
      )
      : Query[A, BagResult] =
      val ret = this.copy()
      ret.requestedJoinOn = requestedJoinOn
      ret.requestedJoinType = requestedJoinType
      ret
  }
  case class Intersect[A: ResultTag]($this: Query[A, ?], $other: Query[A, ?]) extends Query[A, SetResult] {
    override def copyWith
      (
          requestedJoinType: Option[JoinType],
          requestedJoinOn: Option[Fun[A, Expr[Boolean, NonScalarExpr], NonScalarExpr]]
      )
      : Query[A, SetResult] =
      val ret = this.copy()
      ret.requestedJoinOn = requestedJoinOn
      ret.requestedJoinType = requestedJoinType
      ret
  }
  case class IntersectAll[A: ResultTag]($this: Query[A, ?], $other: Query[A, ?]) extends Query[A, BagResult] {
    override def copyWith
      (
          requestedJoinType: Option[JoinType],
          requestedJoinOn: Option[Fun[A, Expr[Boolean, NonScalarExpr], NonScalarExpr]]
      )
      : Query[A, BagResult] =
      val ret = this.copy()
      ret.requestedJoinOn = requestedJoinOn
      ret.requestedJoinType = requestedJoinType
      ret
  }
  case class Except[A: ResultTag]($this: Query[A, ?], $other: Query[A, ?]) extends Query[A, SetResult] {
    override def copyWith
      (
          requestedJoinType: Option[JoinType],
          requestedJoinOn: Option[Fun[A, Expr[Boolean, NonScalarExpr], NonScalarExpr]]
      )
      : Query[A, SetResult] =
      val ret = this.copy()
      ret.requestedJoinOn = requestedJoinOn
      ret.requestedJoinType = requestedJoinType
      ret
  }
  case class ExceptAll[A: ResultTag]($this: Query[A, ?], $other: Query[A, ?]) extends Query[A, BagResult] {
    override def copyWith
      (
          requestedJoinType: Option[JoinType],
          requestedJoinOn: Option[Fun[A, Expr[Boolean, NonScalarExpr], NonScalarExpr]]
      )
      : Query[A, BagResult] =
      val ret = this.copy()
      ret.requestedJoinOn = requestedJoinOn
      ret.requestedJoinType = requestedJoinType
      ret
  }

  // XXX could be either bag or set result, let's leave bag for now
  case class ExprsQuery[A]($values: List[?], $names: List[String])(using ResultTag[A]) extends Query[A, BagResult] {
    override def copyWith
      (
          requestedJoinType: Option[JoinType],
          requestedJoinOn: Option[Fun[A, Expr[Boolean, NonScalarExpr], NonScalarExpr]]
      )
      : Query[A, BagResult] =
      val ret = this.copy()
      ret.requestedJoinOn = requestedJoinOn
      ret.requestedJoinType = requestedJoinType
      ret
  }

  case class ValuesQuery[A]($values: Seq[Tuple], $names: Seq[String])(using ResultTag[A]) extends Query[A, BagResult] {
    override def copyWith
      (
          requestedJoinType: Option[JoinType],
          requestedJoinOn: Option[Fun[A, Expr[Boolean, NonScalarExpr], NonScalarExpr]]
      )
      : Query[A, BagResult] =
      val ret = this.copy()
      ret.requestedJoinOn = requestedJoinOn
      ret.requestedJoinType = requestedJoinType
      ret
  }

  case class NewGroupBy[
      AllSourceTypes <: Tuple,
      ResultType: ResultTag,
      GroupingType,
      GroupingShape <: ExprShape
  ]
    (
        $source: Aggregation[AllSourceTypes, ResultType],
        $grouping: Expr[GroupingType, GroupingShape],
        $sourceRefs: Seq[Ref[?, ?]],
        $sourceTags: collection.Seq[(String, ResultTag[?])],
        $having: Option[Expr[Boolean, ?]]
    ) extends Query[ResultType, BagResult]:
    /** Do not call it filter because having operates on the pre-grouped type.
      */
    def having(havingFn: ToNonScalarRef[AllSourceTypes] => Expr[Boolean, ?]): Query[ResultType, BagResult] =
      if ($having.isEmpty)
        val refsTuple = Tuple.fromArray($sourceRefs.toArray).asInstanceOf[ToNonScalarRef[AllSourceTypes]]

        val havingResult = havingFn(refsTuple)
        NewGroupBy($source, $grouping, $sourceRefs, $sourceTags, Some(havingResult))
      else
        throw new Exception("Error: can only support a single having statement after groupBy")

    override def copyWith
      (
          requestedJoinType: Option[JoinType],
          requestedJoinOn: Option[Fun[ResultType, Expr[Boolean, NonScalarExpr], NonScalarExpr]]
      )
      : Query[ResultType, BagResult] =
      val ret = this.copy()
      ret.requestedJoinOn = requestedJoinOn
      ret.requestedJoinType = requestedJoinType
      ret

  // NOTE: GroupBy is technically an aggregation but will return an interator of at least 1, like a query
  case class GroupBy[
      SourceType,
      ResultType: ResultTag,
      GroupingType,
      GroupingShape <: ExprShape,
      SelectShape <: ExprShape
  ]
    (
        $source: Query[SourceType, ?],
        $groupingFn: Fun[SourceType, Expr[GroupingType, GroupingShape], GroupingShape],
        $selectFn: Fun[SourceType, Expr[ResultType, SelectShape], SelectShape],
        $havingFn: Option[Fun[SourceType, Expr[Boolean, ?], ?]]
    ) extends Query[ResultType, BagResult]:
    /** Do not call it filter because having operates on the pre-grouped type.
      */
    def having(p: Ref[SourceType, ?] => Expr[Boolean, ?]): Query[ResultType, BagResult] =
      if ($havingFn.isEmpty)
        val ref = Ref[SourceType, NonScalarExpr]()(using $source.tag)
        val fun = Fun(ref, p(ref))
        GroupBy($source, $groupingFn, $selectFn, Some(fun))
      else
        throw new Exception("Error: can only support a single having statement after groupBy")

    override def copyWith
      (
          requestedJoinType: Option[JoinType],
          requestedJoinOn: Option[Fun[ResultType, Expr[Boolean, NonScalarExpr], NonScalarExpr]]
      )
      : Query[ResultType, BagResult] =
      val ret = this.copy()
      ret.requestedJoinOn = requestedJoinOn
      ret.requestedJoinType = requestedJoinType
      ret

end Query // object

/* The following is not needed currently

/** A type class for types that can map to a database table */
trait Row:
  type Self
  type Fields = NamedTuple.From[Self]
  type FieldExprs = NamedTuple.Map[Fields, Expr]

  //def toFields(x: Self): Fields = ???
  //def fromFields(x: Fields): Self = ???

 */

package tyql

import scala.annotation.targetName
import language.experimental.namedTuples
import NamedTuple.{AnyNamedTuple, NamedTuple}
import scala.deriving.*
import scala.compiletime.{erasedValue, summonInline}
import tyql.DialectFeature

sealed trait ExprShape
class ScalarExpr extends ExprShape
class NonScalarExpr extends ExprShape

// the compiler cannot understand that this meand that combining with NonScalarExpr means identity
// if you do CalculatedShape[unknownShape, NonScalarExpr] it will not understand that this is equal to unknownShape
type CalculatedShape[S1 <: ExprShape, S2 <: ExprShape] <: ExprShape =
  (S1, S2) match
    case (ScalarExpr, ScalarExpr)       => ScalarExpr
    case (ScalarExpr, NonScalarExpr)    => ScalarExpr
    case (NonScalarExpr, ScalarExpr)    => ScalarExpr
    case (NonScalarExpr, NonScalarExpr) => NonScalarExpr

trait CanBeEqualed[T1, T2]

private[tyql] enum CastTarget:
  case CInt, CString, CDouble, CBool, CFloat, CLong

trait LiteralExpression {}

/** The type of expressions in the query language */
trait Expr[Result, Shape <: ExprShape](using val tag: ResultTag[Result]) extends Selectable:
  /** This type is used to support selection with any of the field names defined by Fields.
    */
  type Fields = NamedTuple.Map[NamedTuple.From[Result], [T] =>> Expr[T, Shape]]

  /** A selection of a field name defined by Fields is implemented by `selectDynamic`. The implementation will add a
    * cast to the right Expr type corresponding to the field type.
    */
  def selectDynamic(fieldName: String) = Expr.Select(this, fieldName)

  /** Member methods to implement universal equality on Expr level. */
  def ==[T, S <: ExprShape]
    (other: Expr[T, S])
    (using CanBeEqualed[Result, T])
    : Expr[Boolean, CalculatedShape[Shape, S]] = Expr.Eq[Shape, S](this, other)
  def ===[T, S <: ExprShape]
    (other: Expr[T, S])
    (using CanBeEqualed[Result, T])
    : Expr[Boolean, CalculatedShape[Shape, S]] = Expr.NullSafeEq[Shape, S](this, other)

  // XXX these are ugly, but hard to remove, since we are running in live Scala, the compiler likes to interpret `==` as a native equality and complain
  def ==(other: String): Expr[Boolean, CalculatedShape[Shape, NonScalarExpr]] = Expr.Eq(this, Expr.StringLit(other))
  def ==(other: Int): Expr[Boolean, CalculatedShape[Shape, NonScalarExpr]] = Expr.Eq(this, Expr.IntLit(other))
  def ==(other: Boolean): Expr[Boolean, CalculatedShape[Shape, NonScalarExpr]] = Expr.Eq(this, Expr.BooleanLit(other))
  def ==(other: Double): Expr[Boolean, CalculatedShape[Shape, NonScalarExpr]] = Expr.Eq(this, Expr.DoubleLit(other))
  def !=(other: String): Expr[Boolean, CalculatedShape[Shape, NonScalarExpr]] = Expr.Ne(this, Expr.StringLit(other))
  def !=(other: Int): Expr[Boolean, CalculatedShape[Shape, NonScalarExpr]] = Expr.Ne(this, Expr.IntLit(other))
  def !=(other: Boolean): Expr[Boolean, CalculatedShape[Shape, NonScalarExpr]] = Expr.Ne(this, Expr.BooleanLit(other))
  def !=(other: Double): Expr[Boolean, CalculatedShape[Shape, NonScalarExpr]] = Expr.Ne(this, Expr.DoubleLit(other))

  def !=[T, Shape2 <: ExprShape]
    (other: Expr[T, Shape2])
    (using CanBeEqualed[Result, T])
    : Expr[Boolean, CalculatedShape[Shape, Shape2]] =
    Expr.Ne[Shape, Shape2](this, other)
  def !==[T, Shape2 <: ExprShape]
    (other: Expr[T, Shape2])
    (using CanBeEqualed[Result, T])
    : Expr[Boolean, CalculatedShape[Shape, Shape2]] =
    Expr.NullSafeNe[Shape, Shape2](this, other)

  def isNull[S <: ExprShape]: Expr[Boolean, Shape] = Expr.IsNull(this)
  def nullIf[S <: ExprShape](other: Expr[Result, S]): Expr[Result, CalculatedShape[Shape, S]] = Expr.NullIf(this, other)

  // TODO why do we need these `asInstanceOf`?
  def asInt: Expr[Int, Shape] =
    Expr.Cast(this, CastTarget.CInt)(using ResultTag.IntTag.asInstanceOf[ResultTag[Int]])
  def asLong: Expr[Long, Shape] =
    Expr.Cast(this, CastTarget.CLong)(using ResultTag.LongTag.asInstanceOf[ResultTag[Long]])
  def asString: Expr[String, Shape] = Expr.Cast[Result, String, Shape](this, CastTarget.CString)(using
  ResultTag.StringTag.asInstanceOf[ResultTag[String]])
  def asDouble: Expr[Double, Shape] = Expr.Cast[Result, Double, Shape](this, CastTarget.CDouble)(using
  ResultTag.DoubleTag.asInstanceOf[ResultTag[Double]])
  def asFloat: Expr[Float, Shape] =
    Expr.Cast[Result, Float, Shape](this, CastTarget.CFloat)(using ResultTag.FloatTag.asInstanceOf[ResultTag[Float]])
  def asBoolean: Expr[Boolean, Shape] =
    Expr.Cast[Result, Boolean, Shape](this, CastTarget.CBool)(using ResultTag.BoolTag.asInstanceOf[ResultTag[Boolean]])

  def cases[DestinationT: ResultTag, SV <: ExprShape]
    (
        firstCase: (Expr[Result, Shape] | ElseToken, Expr[DestinationT, SV]),
        restOfCases: (Expr[Result, Shape] | ElseToken, Expr[DestinationT, SV])*
    )
    : Expr[DestinationT, SV] =
    type FromT = Result
    var mainCases: collection.mutable.ArrayBuffer[(Expr[FromT, Shape], Expr[DestinationT, SV])] =
      collection.mutable.ArrayBuffer.empty
    var elseCase: Option[Expr[DestinationT, SV]] = None
    val cases = Seq(firstCase) ++ restOfCases
    for (((condition, value), index) <- cases.zipWithIndex) {
      condition match
        case _: ElseToken =>
          assert(index == cases.size - 1, "The default condition must be last")
          elseCase = Some(value)
        case _: Expr[?, ?] =>
          mainCases += ((condition.asInstanceOf[Expr[FromT, Shape]], value))
    }
    Expr.SimpleCase(this, mainCases.toList, elseCase)

object Expr:
  /** Sample extension methods for individual types */
  extension [S1 <: ExprShape](x: Expr[Int, S1])
    def %[S2 <: ExprShape](y: Expr[Int, S2]): Expr[Int, CalculatedShape[S1, S2]] = Modulo(x, y)

  extension [S1 <: ExprShape](x: Expr[String, S1])
    def <[S2 <: ExprShape](y: Expr[String, S2]): Expr[Boolean, CalculatedShape[S1, S2]] =
      Lt(x, y)
    def <=[S2 <: ExprShape](y: Expr[String, S2]): Expr[Boolean, CalculatedShape[S1, S2]] =
      Lte(x, y)
    def >[S2 <: ExprShape](y: Expr[String, S2]): Expr[Boolean, CalculatedShape[S1, S2]] =
      Gt(x, y)
    def >=[S2 <: ExprShape](y: Expr[String, S2]): Expr[Boolean, CalculatedShape[S1, S2]] =
      Gte(x, y)
    def between[S2 <: ExprShape, S3 <: ExprShape]
      (min: Expr[String, S2], max: Expr[String, S3])
      : Expr[Boolean, CalculatedShape[S1, CalculatedShape[S2, S3]]] =
      Between(x, min, max)

  extension [T: Numeric, S1 <: ExprShape](x: Expr[T, S1])(using ResultTag[T])
    def <[T2: Numeric, S2 <: ExprShape](y: Expr[T2, S2])(using ResultTag[T2]): Expr[Boolean, CalculatedShape[S1, S2]] =
      Lt(x, y)
    def <=[T2: Numeric, S2 <: ExprShape](y: Expr[T2, S2])(using ResultTag[T2]): Expr[Boolean, CalculatedShape[S1, S2]] =
      Lte(x, y)
    def >[T2: Numeric, S2 <: ExprShape](y: Expr[T2, S2])(using ResultTag[T2]): Expr[Boolean, CalculatedShape[S1, S2]] =
      Gt(x, y)
    def >=[T2: Numeric, S2 <: ExprShape](y: Expr[T2, S2])(using ResultTag[T2]): Expr[Boolean, CalculatedShape[S1, S2]] =
      Gte(x, y)
    def between[S2 <: ExprShape, S3 <: ExprShape]
      (min: Expr[T, S2], max: Expr[T, S3])
      : Expr[Boolean, CalculatedShape[S1, CalculatedShape[S2, S3]]] =
      Between(x, min, max)

    def +[S2 <: ExprShape](y: Expr[T, S2]): Expr[T, CalculatedShape[S1, S2]] = Plus(x, y)
    def -[S2 <: ExprShape](y: Expr[T, S2]): Expr[T, CalculatedShape[S1, S2]] = Minus(x, y)
    def *[S2 <: ExprShape](y: Expr[T, S2]): Expr[T, CalculatedShape[S1, S2]] = Times(x, y)

    def abs: Expr[T, S1] = Abs(x)
    def sqrt: Expr[Double, S1] = Sqrt(x)
    def round: Expr[Int, S1] = Round(x)
    def round[S2 <: ExprShape](precision: Expr[Int, S2]): Expr[Double, CalculatedShape[S1, S2]] =
      RoundWithPrecision(x, precision)
    def ceil: Expr[Int, S1] = Ceil(x)
    def floor: Expr[Int, S1] = Floor(x)
    def power[S2 <: ExprShape](y: Expr[Double, S2]): Expr[Double, CalculatedShape[S1, S2]] = Power(x, y)
    def sign: Expr[Int, S1] = Sign(x)
    def ln: Expr[Double, S1] = LogNatural(x)
    def log(base: Expr[T, S1]): Expr[Double, CalculatedShape[S1, S1]] = Log(base, x)
    def log10: Expr[Double, S1] = Log(IntLit(10), x).asInstanceOf[Expr[Double, S1]] // TODO cast?
    def log2: Expr[Double, S1] = Log(IntLit(2), x).asInstanceOf[Expr[Double, S1]] // TODO cast?

  def exp[T: Numeric, S <: ExprShape](x: Expr[T, S])(using ResultTag[T]): Expr[Double, S] = Exp(x)
  def sin[T: Numeric, S <: ExprShape](x: Expr[T, S])(using ResultTag[T]): Expr[Double, S] = Sin(x)
  def cos[T: Numeric, S <: ExprShape](x: Expr[T, S])(using ResultTag[T]): Expr[Double, S] = Cos(x)
  def tan[T: Numeric, S <: ExprShape](x: Expr[T, S])(using ResultTag[T]): Expr[Double, S] = Tan(x)
  def asin[T: Numeric, S <: ExprShape](x: Expr[T, S])(using ResultTag[T]): Expr[Double, S] = Asin(x)
  def acos[T: Numeric, S <: ExprShape](x: Expr[T, S])(using ResultTag[T]): Expr[Double, S] = Acos(x)
  def atan[T: Numeric, S <: ExprShape](x: Expr[T, S])(using ResultTag[T]): Expr[Double, S] = Atan(x)

  extension [S1 <: ExprShape](x: Expr[Boolean, S1])
    def &&[S2 <: ExprShape](y: Expr[Boolean, S2]): Expr[Boolean, CalculatedShape[S1, S2]] = And(x, y)
    def ||[S2 <: ExprShape](y: Expr[Boolean, S2]): Expr[Boolean, CalculatedShape[S1, S2]] = Or(x, y)
    def unary_! = Not(x)
    def ^(y: Expr[Boolean, S1]): Expr[Boolean, S1] = Xor(x, y)

  extension [S1 <: ExprShape, T](x: Expr[Option[T], S1])(using ResultTag[T])
    def isEmpty: Expr[Boolean, S1] = Expr.IsNull(x)
    def isDefined: Expr[Boolean, S1] = Not(Expr.IsNull(x))
    def get: Expr[T, S1] = x.asInstanceOf[Expr[T, S1]] // TODO should this error silently?
    def getOrElse(default: Expr[T, S1]): Expr[T, S1] = coalesce(x.asInstanceOf[Expr[T, S1]], default)
    def map[U: ResultTag, S2 <: ExprShape](f: Ref[T, NonScalarExpr] => Expr[U, NonScalarExpr]): Expr[Option[U], S1] =
      OptionMap(x, f)
    // TODO unclear how to implement flatMap
    // TODO somehow use options in aggregations

  extension [S1 <: ExprShape](x: Expr[Array[Byte], S1])
    @targetName("byteLengthForArrayByte")
    def byteLength: Expr[Int, S1] =
      Expr.ByteByteLength(x.asInstanceOf[Expr[(Array[Byte] | Function0[java.io.InputStream]), S1]])
  extension [S1 <: ExprShape](x: Expr[() => java.io.InputStream, S1])
    @targetName("byteLengthForjavaioInputStream")
    def byteLength: Expr[Int, S1] =
      Expr.ByteByteLength(x.asInstanceOf[Expr[(Array[Byte] | Function0[java.io.InputStream]), S1]])

  extension [S1 <: ExprShape](x: Expr[String, S1])
    def toLowerCase: Expr[String, S1] = Expr.Lower(x)
    def toUpperCase: Expr[String, S1] = Expr.Upper(x)
    def charLength: Expr[Int, S1] = Expr.StringCharLength(x)
    def length: Expr[Int, S1] = charLength
    def byteLength: Expr[Int, S1] = Expr.StringByteLength(x)
    def stripLeading: Expr[String, S1] = Expr.LTrim(x) // Java naming
    def stripTrailing: Expr[String, S1] = Expr.RTrim(x) // Java naming
    def strip: Expr[String, S1] = Expr.Trim(x) // Java naming
    def ltrim: Expr[String, S1] = Expr.LTrim(x) // SQL naming
    def rtrim: Expr[String, S1] = Expr.RTrim(x) // SQL naming
    def trim: Expr[String, S1] = Expr.Trim(x) // SQL naming
    def replace[S2 <: ExprShape](from: Expr[String, S2], to: Expr[String, S2]): Expr[String, CalculatedShape[S1, S2]] =
      Expr.StrReplace(x, from, to)
    // TODO maybe add assertions that len should be >= 0 and from >= 1 if we know them?
    // SQL semantics (1-based indexing, start+length)
    def substr[S2 <: ExprShape](from: Expr[Int, S2], len: Expr[Int, S2] = null): Expr[String, CalculatedShape[S1, S2]] =
      Expr.Substring(x, from, Option.fromNullable(len))
    // Java semantics (0-based indexing, start+afterLast)
    def substring[S2 <: ExprShape]
      (start: Expr[Int, S2], afterLast: Expr[Int, S2] = null)
      : Expr[String, CalculatedShape[S1, S2]] =
      if afterLast != null then
        substr(
          Expr.Plus(Expr.IntLit(1), start).asInstanceOf[Expr[Int, S2]],
          Expr.Minus(afterLast, start).asInstanceOf[Expr[Int, S2]]
        ) // XXX how to avoid this cast
      else
        substr(Expr.Plus(Expr.IntLit(1), start).asInstanceOf[Expr[Int, S2]], null) // XXX how to avoid this cast
    def like[S2 <: ExprShape](pattern: Expr[String, S2]): Expr[Boolean, CalculatedShape[S1, S2]] =
      Expr.StrLike(x, pattern)
    def `+`[S2 <: ExprShape](y: Expr[String, S2]): Expr[String, CalculatedShape[S1, S2]] = Expr.StrConcat(x, Seq(y))
    def reverse(using DialectFeature.ReversibleStrings): Expr[String, S1] = Expr.StrReverse(x)
    def repeat[S2 <: ExprShape](n: Expr[Int, S2]): Expr[String, CalculatedShape[S1, S2]] = Expr.StrRepeat(x, n)
    def lpad[S2 <: ExprShape](len: Expr[Int, S2], pad: Expr[String, S2]): Expr[String, CalculatedShape[S1, S2]] =
      Expr.StrLPad(x, len, pad)
    def rpad[S2 <: ExprShape](len: Expr[Int, S2], pad: Expr[String, S2]): Expr[String, CalculatedShape[S1, S2]] =
      Expr.StrRPad(x, len, pad)
    def findPosition[S2 <: ExprShape](substr: Expr[String, S2]): Expr[Int, CalculatedShape[S1, S2]] =
      Expr.StrPositionIn(substr, x)

  def coalesce[T, S1 <: ExprShape](x: Expr[T, S1], y: Expr[T, S1], xs: Expr[T, S1]*)(using ResultTag[T]): Expr[T, S1] =
    Coalesce(x, y, xs)
  def nullIf[T, S1 <: ExprShape, S2 <: ExprShape]
    (x: Expr[T, S1], y: Expr[T, S2])
    (using ResultTag[T])
    : Expr[T, CalculatedShape[S1, S2]] = NullIf(x, y)

  def concat[S <: ExprShape](strs: Seq[Expr[String, S]]): Expr[String, S] =
    assert(strs.nonEmpty, "concat requires at least one argument")
    StrConcatUniform(strs.head, strs.tail)
  // TODO XXX this cannot be named concat since then Scala will never resolve it, it will always try for the first version without the sep parameter.
  def concatWith[S <: ExprShape, SS <: ExprShape]
    (strs: Seq[Expr[String, S]], sep: Expr[String, SS])
    : Expr[String, CalculatedShape[S, SS]] =
    assert(strs.nonEmpty, "concatWith requires at least one argument")
    StrConcatSeparator(sep, strs.head, strs.tail)

  extension [A](x: Expr[List[A], NonScalarExpr])(using ResultTag[List[A]])
    def prepend(elem: Expr[A, NonScalarExpr]): Expr[List[A], NonScalarExpr] = ListPrepend(elem, x)
    def append(elem: Expr[A, NonScalarExpr]): Expr[List[A], NonScalarExpr] = ListAppend(x, elem)
    // XXX Due to Scala overloading bugs, there can be no two extensions methods named `contains` with similar arguments.
    // XXX Because the list one is less used, this one is called `containsElement` instead.
    def containsElement(elem: Expr[A, NonScalarExpr]): Expr[Boolean, NonScalarExpr] = ListContains(x, elem)
    def length: Expr[Int, NonScalarExpr] = ListLength(x)

  // Aggregations
  def sum(x: Expr[Int, ?]): AggregationExpr[Int] = AggregationExpr.Sum(x) // TODO: require summable type?
  @targetName("doubleSum")
  def sum(x: Expr[Double, ?]): AggregationExpr[Double] = AggregationExpr.Sum(x) // TODO: require summable type?
  def avg[T: ResultTag](x: Expr[T, ?]): AggregationExpr[T] = AggregationExpr.Avg(x)
  @targetName("doubleAvg")
  def avg(x: Expr[Double, ?]): AggregationExpr[Double] = AggregationExpr.Avg(x)
  def max[T: ResultTag](x: Expr[T, ?]): AggregationExpr[T] = AggregationExpr.Max(x)
  def min[T: ResultTag](x: Expr[T, ?]): AggregationExpr[T] = AggregationExpr.Min(x)
  def count(x: Expr[Int, ?]): AggregationExpr[Int] = AggregationExpr.Count(x)
  @targetName("stringCnt")
  def count(x: Expr[String, ?]): AggregationExpr[Int] = AggregationExpr.Count(x)

  // Window function expressions
  def rowNumber: ExprInWindowPosition[Int] = RowNumber()
  def rank: ExprInWindowPosition[Int] = Rank()
  def denseRank: ExprInWindowPosition[Int] = DenseRank()
  def ntile(n: Int): ExprInWindowPosition[Int] = NTile(n)
  def lag[R, S <: ExprShape]
    (e: Expr[R, S], offset: Int, default: Expr[R, S])
    (using ResultTag[R])
    : ExprInWindowPosition[R] = Lag(e, Some(offset), Some(default))
  def lag[R, S <: ExprShape](e: Expr[R, S], offset: Int)(using ResultTag[R]): ExprInWindowPosition[R] =
    Lag(e, Some(offset), None)
  def lag[R, S <: ExprShape](e: Expr[R, S])(using ResultTag[R]): ExprInWindowPosition[R] = Lag(e, None, None)
  def lead[R, S <: ExprShape]
    (e: Expr[R, S], offset: Int, default: Expr[R, S])
    (using ResultTag[R])
    : ExprInWindowPosition[R] = Lead(e, Some(offset), Some(default))
  def lead[R, S <: ExprShape](e: Expr[R, S], offset: Int)(using ResultTag[R]): ExprInWindowPosition[R] =
    Lead(e, Some(offset), None)
  def lead[R, S <: ExprShape](e: Expr[R, S])(using ResultTag[R]): ExprInWindowPosition[R] = Lead(e, None, None)
  def firstValue[R, S <: ExprShape](e: Expr[R, S])(using ResultTag[R]): ExprInWindowPosition[R] = FirstValue(e)
  def lastValue[R, S <: ExprShape](e: Expr[R, S])(using ResultTag[R]): ExprInWindowPosition[R] = LastValue(e)
  def nthValue[R, S <: ExprShape](e: Expr[R, S], n: Int)(using ResultTag[R]): ExprInWindowPosition[R] = NthValue(e, n)

  // TODO aren't these types too restrictive?
  def cases[T: ResultTag, SC <: ExprShape, SV <: ExprShape]
    (
        firstCase: (Expr[Boolean, SC] | true | ElseToken, Expr[T, SV]),
        restOfCases: (Expr[Boolean, SC] | true | ElseToken, Expr[T, SV])*
    )
    : Expr[T, SV] =
    var mainCases: collection.mutable.ArrayBuffer[(Expr[Boolean, SC], Expr[T, SV])] =
      collection.mutable.ArrayBuffer.empty
    var elseCase: Option[Expr[T, SV]] = None
    val cases = Seq(firstCase) ++ restOfCases
    for (((condition, value), index) <- cases.zipWithIndex) {
      condition match
        case _: ElseToken =>
          assert(index == cases.size - 1, "The default condition must be last")
          elseCase = Some(value)
        case true =>
          assert(index == cases.size - 1, "The default condition must be last")
          elseCase = Some(value)
        case false => assert(false, "what do you mean, false?")
        case _: Expr[?, ?] =>
          mainCases += ((condition.asInstanceOf[Expr[Boolean, SC]], value))
    }
    SearchedCase(mainCases.toList, elseCase)

  // Note: All field names of constructors in the query language are prefixed with `$`
  // so that we don't accidentally pick a field name of a constructor class where we want
  // a name in the domain model instead.

  // These case classes could technically contain e.g. 1 < 'a', but there are no user-exposes functions to create such expressions
  type IsString[T] = T =:= String
  type Comparable[T] = Numeric[T] | IsString[T]
  case class Lt[T1: Comparable, T2: Comparable, S1 <: ExprShape, S2 <: ExprShape]
    ($x: Expr[T1, S1], $y: Expr[T2, S2])
    (using ResultTag[T1], ResultTag[T2]) extends Expr[Boolean, CalculatedShape[S1, S2]]
  case class Lte[T1: Comparable, T2: Comparable, S1 <: ExprShape, S2 <: ExprShape]
    ($x: Expr[T1, S1], $y: Expr[T2, S2])
    (using ResultTag[T1], ResultTag[T2]) extends Expr[Boolean, CalculatedShape[S1, S2]]
  case class Gt[T1: Comparable, T2: Comparable, S1 <: ExprShape, S2 <: ExprShape]
    ($x: Expr[T1, S1], $y: Expr[T2, S2])
    (using ResultTag[T1], ResultTag[T2]) extends Expr[Boolean, CalculatedShape[S1, S2]]
  case class Gte[T1: Comparable, T2: Comparable, S1 <: ExprShape, S2 <: ExprShape]
    ($x: Expr[T1, S1], $y: Expr[T2, S2])
    (using ResultTag[T1], ResultTag[T2]) extends Expr[Boolean, CalculatedShape[S1, S2]]
  case class Between[T: Comparable, S1 <: ExprShape, S2 <: ExprShape, S3 <: ExprShape]
    ($x: Expr[T, S1], $min: Expr[T, S2], $max: Expr[T, S3])
    (using ResultTag[T]) extends Expr[Boolean, CalculatedShape[S1, CalculatedShape[S2, S3]]]

  // TODO maybe remove these FunctionCall_, for now they only exist to create fake AST trees for debug purposes...
  case class FunctionCall0[R](name: String)(using ResultTag[R])
      extends Expr[R, NonScalarExpr] // XXX TODO NonScalarExpr?
  case class FunctionCall1[A1, R, S1 <: ExprShape](name: String, $a1: Expr[A1, S1])(using ResultTag[R])
      extends Expr[R, S1]
  case class FunctionCall2[A1, A2, R, S1 <: ExprShape, S2 <: ExprShape]
    (name: String, $a1: Expr[A1, S1], $a2: Expr[A2, S2])
    (using ResultTag[R]) extends Expr[R, CalculatedShape[S1, S2]]

  case class Plus[S1 <: ExprShape, S2 <: ExprShape, T: Numeric]($x: Expr[T, S1], $y: Expr[T, S2])(using ResultTag[T])
      extends Expr[T, CalculatedShape[S1, S2]]
  case class Minus[S1 <: ExprShape, S2 <: ExprShape, T: Numeric]($x: Expr[T, S1], $y: Expr[T, S2])(using ResultTag[T])
      extends Expr[T, CalculatedShape[S1, S2]]
  case class Times[S1 <: ExprShape, S2 <: ExprShape, T: Numeric]($x: Expr[T, S1], $y: Expr[T, S2])(using ResultTag[T])
      extends Expr[T, CalculatedShape[S1, S2]]

  case class And[S1 <: ExprShape, S2 <: ExprShape]($x: Expr[Boolean, S1], $y: Expr[Boolean, S2])
      extends Expr[Boolean, CalculatedShape[S1, S2]]
  case class Or[S1 <: ExprShape, S2 <: ExprShape]($x: Expr[Boolean, S1], $y: Expr[Boolean, S2])
      extends Expr[Boolean, CalculatedShape[S1, S2]]
  case class Not[S1 <: ExprShape]($x: Expr[Boolean, S1]) extends Expr[Boolean, S1]
  case class Xor[S1 <: ExprShape]($x: Expr[Boolean, S1], $y: Expr[Boolean, S1]) extends Expr[Boolean, S1]

  case class ByteByteLength[S <: ExprShape]($x: Expr[(Array[Byte] | Function0[java.io.InputStream]), S])
      extends Expr[Int, S]

  case class Upper[S <: ExprShape]($x: Expr[String, S]) extends Expr[String, S]
  case class Lower[S <: ExprShape]($x: Expr[String, S]) extends Expr[String, S]
  case class StringCharLength[S <: ExprShape]($x: Expr[String, S]) extends Expr[Int, S]
  case class StringByteLength[S <: ExprShape]($x: Expr[String, S]) extends Expr[Int, S]
  case class Trim[S <: ExprShape]($x: Expr[String, S]) extends Expr[String, S]
  case class LTrim[S <: ExprShape]($x: Expr[String, S]) extends Expr[String, S]
  case class RTrim[S <: ExprShape]($x: Expr[String, S]) extends Expr[String, S]
  case class StrReplace[S <: ExprShape, S2 <: ExprShape]
    ($s: Expr[String, S], $from: Expr[String, S2], $to: Expr[String, S2]) extends Expr[String, CalculatedShape[S, S2]]
  case class Substring[S <: ExprShape, S2 <: ExprShape]
    ($s: Expr[String, S], $from: Expr[Int, S2], $len: Option[Expr[Int, S2]])
      extends Expr[String, CalculatedShape[S, S2]]
  case class StrLike[S <: ExprShape, S2 <: ExprShape]($s: Expr[String, S], $pattern: Expr[String, S2])
      extends Expr[Boolean, CalculatedShape[S, S2]] // NonScalar like StringLit
  case class StrConcat[S1 <: ExprShape, S2 <: ExprShape]($x: Expr[String, S1], $xs: Seq[Expr[String, S2]])
      extends Expr[String, CalculatedShape[S1, S2]] // First one has a different shape so you can use it as an opertor between two arguments that have different shapes
  case class StrConcatUniform[S1 <: ExprShape]($x: Expr[String, S1], $xs: Seq[Expr[String, S1]])
      extends Expr[String, S1]
  case class StrConcatSeparator[S1 <: ExprShape, S3 <: ExprShape]
    ($sep: Expr[String, S3], $x: Expr[String, S1], $xs: Seq[Expr[String, S1]])
      extends Expr[String, CalculatedShape[S1, S3]]
  case class StrReverse[S <: ExprShape]($x: Expr[String, S]) extends Expr[String, S]
  case class StrRepeat[S1 <: ExprShape, S2 <: ExprShape]($s: Expr[String, S1], $n: Expr[Int, S2])
      extends Expr[String, CalculatedShape[S1, S2]]
  case class StrLPad[S1 <: ExprShape, S2 <: ExprShape]
    ($s: Expr[String, S1], $len: Expr[Int, S2], $pad: Expr[String, S2]) extends Expr[String, CalculatedShape[S1, S2]]
  case class StrRPad[S1 <: ExprShape, S2 <: ExprShape]
    ($s: Expr[String, S1], $len: Expr[Int, S2], $pad: Expr[String, S2]) extends Expr[String, CalculatedShape[S1, S2]]
  case class StrPositionIn[S1 <: ExprShape, S2 <: ExprShape]($substr: Expr[String, S2], $string: Expr[String, S1])
      extends Expr[Int, CalculatedShape[S1, S2]]

  case class Modulo[S1 <: ExprShape, S2 <: ExprShape]($x: Expr[Int, S1], $y: Expr[Int, S2])
      extends Expr[Int, CalculatedShape[S1, S2]]
  // TODO actually, it's unclear for now what types should be here, the input to ROUND() in most DBs can be any numeric and the ouput is usually of the same type as the input
  case class Round[S1 <: ExprShape, T: Numeric]($x: Expr[T, S1])(using ResultTag[T]) extends Expr[Int, S1]
  case class RoundWithPrecision[S1 <: ExprShape, S2 <: ExprShape, T: Numeric]
    ($x: Expr[T, S1], $precision: Expr[Int, S2])
    (using ResultTag[T]) extends Expr[Double, CalculatedShape[S1, S2]]
  case class Ceil[S1 <: ExprShape, T: Numeric]($x: Expr[T, S1])(using ResultTag[T]) extends Expr[Int, S1]
  case class Floor[S1 <: ExprShape, T: Numeric]($x: Expr[T, S1])(using ResultTag[T]) extends Expr[Int, S1]
  case class Power[S1 <: ExprShape, S2 <: ExprShape, T1: Numeric, T2: Numeric]
    ($x: Expr[T1, S1], $y: Expr[T2, S2])
    (using ResultTag[T1], ResultTag[T2]) extends Expr[Double, CalculatedShape[S1, S2]]
  case class Sqrt[S1 <: ExprShape, T: Numeric]($x: Expr[T, S1])(using ResultTag[T]) extends Expr[Double, S1]
  case class Abs[S1 <: ExprShape, T: Numeric]($x: Expr[T, S1])(using ResultTag[T]) extends Expr[T, S1]
  case class Sign[S1 <: ExprShape, T: Numeric]($x: Expr[T, S1])(using ResultTag[T]) extends Expr[Int, S1]
  case class LogNatural[S1 <: ExprShape, T: Numeric]($x: Expr[T, S1])(using ResultTag[T]) extends Expr[Double, S1]
  case class Log[S1 <: ExprShape, S2 <: ExprShape, T1: Numeric, T2: Numeric]
    ($base: Expr[T1, S1], $x: Expr[T2, S2])
    (using ResultTag[T1], ResultTag[T2]) extends Expr[Double, CalculatedShape[S1, S2]]
  case class Exp[S1 <: ExprShape, T: Numeric]($x: Expr[T, S1])(using ResultTag[T]) extends Expr[Double, S1]
  case class Sin[S1 <: ExprShape, T: Numeric]($x: Expr[T, S1])(using ResultTag[T]) extends Expr[Double, S1]
  case class Cos[S1 <: ExprShape, T: Numeric]($x: Expr[T, S1])(using ResultTag[T]) extends Expr[Double, S1]
  case class Tan[S1 <: ExprShape, T: Numeric]($x: Expr[T, S1])(using ResultTag[T]) extends Expr[Double, S1]
  case class Asin[S1 <: ExprShape, T: Numeric]($x: Expr[T, S1])(using ResultTag[T]) extends Expr[Double, S1]
  case class Acos[S1 <: ExprShape, T: Numeric]($x: Expr[T, S1])(using ResultTag[T]) extends Expr[Double, S1]
  case class Atan[S1 <: ExprShape, T: Numeric]($x: Expr[T, S1])(using ResultTag[T]) extends Expr[Double, S1]

  case class RandomUUID() extends Expr[String, NonScalarExpr] // XXX NonScalarExpr?
  case class RandomFloat() extends Expr[Double, NonScalarExpr] // XXX NonScalarExpr?
  case class RandomInt[S1 <: ExprShape, S2 <: ExprShape]($x: Expr[Int, S1], $y: Expr[Int, S2])
      extends Expr[Int, CalculatedShape[S1, S2]]

  case class ListExpr[A]($elements: List[Expr[A, NonScalarExpr]])(using ResultTag[List[A]])
      extends Expr[List[A], NonScalarExpr]
  extension [A, E <: Expr[A, NonScalarExpr]](x: List[E])
    def toExpr(using ResultTag[List[A]]): ListExpr[A] = ListExpr(x)
  //  given Conversion[List[A], ListExpr[A]] = ListExpr(_)

  case class ListPrepend[A]($x: Expr[A, NonScalarExpr], $list: Expr[List[A], NonScalarExpr])(using ResultTag[List[A]])
      extends Expr[List[A], NonScalarExpr]
  case class ListAppend[A]($list: Expr[List[A], NonScalarExpr], $x: Expr[A, NonScalarExpr])(using ResultTag[List[A]])
      extends Expr[List[A], NonScalarExpr]
  case class ListContains[A]($list: Expr[List[A], NonScalarExpr], $x: Expr[A, NonScalarExpr])(using ResultTag[Boolean])
      extends Expr[Boolean, NonScalarExpr]
  case class ListLength[A]($list: Expr[List[A], NonScalarExpr])(using ResultTag[Int]) extends Expr[Int, NonScalarExpr]

  // So far Select is weakly typed, so `selectDynamic` is easy to implement.
  // TODO: Make it strongly typed like the other cases
  case class Select[A: ResultTag]($x: Expr[A, ?], $name: String)
      extends Expr[
        A,
        NonScalarExpr
      ] // TODO is this type correct? X is of type A and it's child under `name` is also of type A?

  case class Concat[A <: AnyNamedTuple, B <: AnyNamedTuple, S1 <: ExprShape, S2 <: ExprShape]
    ($x: Expr[A, S1], $y: Expr[B, S2])
    (using ResultTag[NamedTuple.Concat[A, B]]) extends Expr[NamedTuple.Concat[A, B], CalculatedShape[S1, S2]]

  case class Project[A <: AnyNamedTuple]($a: A)(using ResultTag[NamedTuple.Map[A, StripExpr]])
      extends Expr[NamedTuple.Map[A, StripExpr], NonScalarExpr]

  type StripExpr[E] = E match
    case Expr[b, s]         => b
    case AggregationExpr[b] => b
    case _                  => E

  // Also weakly typed in the arguments since these two classes model universal equality */
  case class Eq[S1 <: ExprShape, S2 <: ExprShape]($x: Expr[?, S1], $y: Expr[?, S2])
      extends Expr[Boolean, CalculatedShape[S1, S2]]
  case class Ne[S1 <: ExprShape, S2 <: ExprShape]($x: Expr[?, S1], $y: Expr[?, S2])
      extends Expr[Boolean, CalculatedShape[S1, S2]]
  case class NullSafeEq[S1 <: ExprShape, S2 <: ExprShape]($x: Expr[?, S1], $y: Expr[?, S2])
      extends Expr[Boolean, CalculatedShape[S1, S2]]
  case class NullSafeNe[S1 <: ExprShape, S2 <: ExprShape]($x: Expr[?, S1], $y: Expr[?, S2])
      extends Expr[Boolean, CalculatedShape[S1, S2]]

  // Expressions resulting from queries
  // Cannot use Contains with an aggregation
  case class Contains[A]($query: Query[A, ?], $expr: Expr[A, NonScalarExpr]) extends Expr[Boolean, NonScalarExpr]
  case class IsEmpty[A]($this: Query[A, ?]) extends Expr[Boolean, NonScalarExpr]
  case class NonEmpty[A]($this: Query[A, ?]) extends Expr[Boolean, NonScalarExpr]

  /** References are placeholders for parameters */
  private var refCount = 0 // TODO: do we want to recount from 0 for each query?
  private var exprRefCount = 0

  // References to relations
  case class Ref[A: ResultTag, S <: ExprShape](idx: Int = -1) extends Expr[A, S]:
    private val $id = refCount
    refCount += 1
    val idxStr = if idx == -1 then "" else s"_$idx"
    def stringRef() = s"ref${$id}$idxStr"
    override def toString: String = s"Ref[${stringRef()}]$idxStr"

  /** The internal representation of a function `A => B` Query languages are usually first-order, so Fun is not an Expr
    */
  case class Fun[A, B, S <: ExprShape]($param: Ref[A, S], $body: B)

  // TODO aren't these types too restrictive?
  case class SearchedCase[T, SC <: ExprShape, SV <: ExprShape]
    ($cases: List[(Expr[Boolean, SC], Expr[T, SV])], $else: Option[Expr[T, SV]])
    (using ResultTag[T]) extends Expr[T, SV]
  case class SimpleCase[TE, TR, SE <: ExprShape, SR <: ExprShape]
    ($expr: Expr[TE, SE], $cases: List[(Expr[TE, SE], Expr[TR, SR])], $else: Option[Expr[TR, SR]])
    (using ResultTag[TE], ResultTag[TR]) extends Expr[TR, SR]

  case class OptionMap[A, B, S <: ExprShape]
    ($x: Expr[Option[A], S], $f: Ref[A, NonScalarExpr] => Expr[B, NonScalarExpr])
    (using ResultTag[A], ResultTag[B]) extends Expr[Option[B], S]

  case class Cast[A, B, S <: ExprShape]($x: Expr[A, S], resultType: CastTarget)(using ResultTag[B]) extends Expr[B, S]

  case class NullLit[A]()(using ResultTag[A]) extends Expr[A, NonScalarExpr] with LiteralExpression
  case class IsNull[A, S <: ExprShape]($x: Expr[A, S]) extends Expr[Boolean, S]
  case class Coalesce[A, S1 <: ExprShape]($x1: Expr[A, S1], $x2: Expr[A, S1], $xs: Seq[Expr[A, S1]])(using ResultTag[A])
      extends Expr[A, S1]
  case class NullIf[A, S1 <: ExprShape, S2 <: ExprShape]($x: Expr[A, S1], $y: Expr[A, S2])(using ResultTag[A])
      extends Expr[A, CalculatedShape[S1, S2]]

  /** Literals are type-specific, tailored to the types that the DB supports */
  case class BytesLit($value: Array[Byte]) extends Expr[Array[Byte], NonScalarExpr]
  case class ByteStreamLit($value: () => java.io.InputStream) extends Expr[() => java.io.InputStream, NonScalarExpr]

  case class IntLit($value: Int) extends Expr[Int, NonScalarExpr] with LiteralExpression
  case class LongLit($value: Long) extends Expr[Long, NonScalarExpr] with LiteralExpression

  /** Scala values can be lifted into literals by conversions */
  given Conversion[Int, IntLit] = IntLit(_)
  // XXX maybe only from literals with FromDigits?

  case class StringLit($value: String) extends Expr[String, NonScalarExpr]
      with LiteralExpression // TODO XXX why is this nonscalar?
  given Conversion[String, StringLit] = StringLit(_)

  case class DoubleLit($value: Double) extends Expr[Double, NonScalarExpr] with LiteralExpression
  given Conversion[Double, DoubleLit] = DoubleLit(_)

  case class FloatLit($value: Float) extends Expr[Float, NonScalarExpr] with LiteralExpression
  given Conversion[Float, FloatLit] = FloatLit(_)

  case class BooleanLit($value: Boolean) extends Expr[Boolean, NonScalarExpr] with LiteralExpression
  //  given Conversion[Boolean, BooleanLit] = BooleanLit(_)
  // TODO why does this break things?

  def randomFloat(): Expr[Double, NonScalarExpr] = RandomFloat()
  def randomUUID(using r: DialectFeature.RandomUUID)(): Expr[String, NonScalarExpr] = RandomUUID()
  def randomInt[S1 <: ExprShape, S2 <: ExprShape]
    (a: Expr[Int, S1], b: Expr[Int, S2])
    (using r: DialectFeature.RandomIntegerInInclusiveRange)
    : Expr[Int, CalculatedShape[S1, S2]] =
    // TODO maybe add a check for (a <= b) if we know both components at generation time?
    // TODO what about parentheses? Do we really not need them?
    RandomInt(a, b)

  /** Should be able to rely on the implicit conversions, but not always. One approach is to overload, another is to
    * provide a user-facing toExpr function.
    */
//  def toExpr[T](t: T): Expr[T, NonScalarExpr] = t match
//    case t:Int => IntLit(t)
//    case t:Double => DoubleLit(t)
//    case t:String => StringLit(t)
//    case t:Boolean => BooleanLit(t)

  /* ABSTRACTION: if we want to abstract over expressions (not relations) in the DSL, to enable better composability,
  then the DSL needs some kind of abstraction/application operation.
  Option 1: (already supported) use host-level abstraction e.g. define a lambda.
  Option 2: (below) define a substitution method, WIP.
  Option 3: Use a macro to do substitution, but then lose the macro-free claim.
   */
  case class RefExpr[A: ResultTag, S <: ExprShape]() extends Expr[A, S]:
    private val id = exprRefCount
    exprRefCount += 1
    def stringRef() = s"exprRef$id"
    override def toString: String = s"ExprRef[${stringRef()}]"

  case class AbstractedExpr[A, B, S <: ExprShape]($param: RefExpr[A, S], $body: Expr[B, S]):
    def apply(exprArg: Expr[A, S]): Expr[B, S] =
      substitute($body, $param, exprArg)
    private def substitute[C](expr: Expr[B, S], formalP: RefExpr[A, S], actualP: Expr[A, S]): Expr[B, S] = ???
  type Pred[A, S <: ExprShape] = Fun[A, Expr[Boolean, S], S]

  type IsTupleOfExpr[A <: AnyNamedTuple] = Tuple.Union[NamedTuple.DropNames[A]] <:< Expr[?, NonScalarExpr]

  /** Explicit conversion from (name_1: Expr[T_1], ..., name_n: Expr[T_n]) to Expr[(name_1: T_1, ..., name_n: T_n)]
    */
  extension [A <: AnyNamedTuple : IsTupleOfExpr](x: A)
    def toRow(using ResultTag[NamedTuple.Map[A, StripExpr]]): Project[A] = Project(x)

// TODO: use NamedTuple.from to convert case classes to named tuples before using concat
  extension [A <: AnyNamedTuple, S <: ExprShape](x: Expr[A, S])
    def concat[B <: AnyNamedTuple, S2 <: ExprShape]
      (other: Expr[B, S2])
      (using ResultTag[NamedTuple.Concat[A, B]])
      : Expr[NamedTuple.Concat[A, B], CalculatedShape[S, S2]] = Concat(x, other)

  /** Same as _.toRow, as an implicit conversion */
//  given [A <: AnyNamedTuple : IsTupleOfExpr](using ResultTag[NamedTuple.Map[A, StripExpr]]): Conversion[A, Expr.Project[A]] = Expr.Project(_)

end Expr

inline def lit(x: () => java.io.InputStream): Expr[() => java.io.InputStream, NonScalarExpr] = Expr.ByteStreamLit(x)
inline def lit(x: java.io.InputStream): Expr[() => java.io.InputStream, NonScalarExpr] = Expr.ByteStreamLit(() => x)
inline def lit(x: Array[Byte]): Expr[Array[Byte], NonScalarExpr] = Expr.BytesLit(x)
inline def lit(x: Int): Expr[Int, NonScalarExpr] & LiteralExpression = Expr.IntLit(x)
inline def lit(x: Long): Expr[Long, NonScalarExpr] & LiteralExpression = Expr.LongLit(x)
inline def lit(x: Double): Expr[Double, NonScalarExpr] & LiteralExpression = Expr.DoubleLit(x)
inline def lit(x: Float): Expr[Float, NonScalarExpr] & LiteralExpression = Expr.FloatLit(x)
inline def lit(x: String): Expr[String, NonScalarExpr] & LiteralExpression = Expr.StringLit(x)
inline def lit(x: Boolean): Expr[Boolean, NonScalarExpr] & LiteralExpression = Expr.BooleanLit(x)
inline def True = Expr.BooleanLit(true)
inline def False = Expr.BooleanLit(false)
inline def Null = Expr.NullLit[scala.Null]()
// TODO a good place for implicitNotFound
def Null[T](using ResultTag[T]) = Expr.NullLit[T]()
private case class ElseToken()
val Else = new ElseToken()

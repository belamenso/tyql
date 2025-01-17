package tyql

import scala.annotation.elidable
import tyql.DialectFeature
import tyql.DialectFeature.*

private def unsupportedFeature(feature: String) =
  throw new UnsupportedOperationException(s"$feature feature not supported in this dialect!")

// ╔════════════════════════════════════════════════════════════════════╗
// ║                         Base Dialect Trait                         ║
// ╚════════════════════════════════════════════════════════════════════╝
trait Dialect:
  def name(): String

  protected val reservedKeywords: Set[String]
  def quoteIdentifier(id: String): String

  def limitAndOffset(limit: Long, offset: Long): String

  def quoteStringLiteral(in: String, insideLikePattern: Boolean): String

  val stringLengthByCharacters: String = "CHAR_LENGTH"
  val stringLengthBytesNeedsEncodeFirst: Boolean = false

  val xorOperatorSupportedNatively = false

  def feature_RandomUUID_functionName: String = unsupportedFeature("RandomUUID")
  def feature_RandomFloat_functionName: Option[String] = throw new UnsupportedOperationException("RandomFloat")
  def feature_RandomFloat_rawSQL: Option[SqlSnippet] = throw new UnsupportedOperationException("RandomFloat")
  def feature_RandomInt_rawSQL: SqlSnippet = unsupportedFeature("RandomInt")

  def needsStringRepeatPolyfill: Boolean = false
  def needsStringLPadRPadPolyfill: Boolean = false

  def stringPositionFindingVia: String = "LOCATE"

  val nullSafeEqualityViaSpecialOperator: Boolean = false

  val booleanCast: String = "BOOLEAN"
  val integerCast: String = "INTEGER"
  val doubleCast: String = "DOUBLE PRECISION"
  val stringCast: String = "VARCHAR"
  val floatCast: String = "FLOAT"
  val longCast: String = "BIGINT"

  val `prefers $n over ? for parametrization` = false

  def shouldHaveParensInsideValuesExpr: Boolean = true

  def needsByteByByteEscapingOfBlobs: Boolean = false

  def acceptsLimitInDeleteQueries: Boolean = false

  def canExtractDateTimeComponentsNatively: Boolean = true

  def arrayLengthFunctionName: String = "ARRAY_LENGTH"
  def arrayPrependFunctionName: String = "ARRAY_PREPEND"

object Dialect:
  val literal_percent = '\uE000'
  val literal_underscore = '\uE001'

  given Dialect = new Dialect
    with QuotingIdentifiers.AnsiBehavior
    with LimitAndOffset.Separate
    with StringLiteral.AnsiSingleQuote:
    def name() = "ANSI SQL Dialect"

// ╔════════════════════════════════════════════════════════════════════╗
// ║                         ANSI SQL Dialect                           ║
// ╚════════════════════════════════════════════════════════════════════╝
  object ansi:
    given Dialect = Dialect.given_Dialect
    given [T: ResultTag]: CanBeEqualed[T, T] = new CanBeEqualed[T, T] {}
    given [T1: Numeric, T2: Numeric]: CanBeEqualed[T1, T2] = new CanBeEqualed[T1, T2] {}
    given INCanHandleRows = new INCanHandleRows {}

// ╔════════════════════════════════════════════════════════════════════╗
// ║                        PostgreSQL Dialect                          ║
// ╚════════════════════════════════════════════════════════════════════╝
  object postgresql:
    given Dialect = new Dialect
      with QuotingIdentifiers.PostgresqlBehavior
      with LimitAndOffset.Separate
      with StringLiteral.PostgresqlBehavior:
      def name() = "PostgreSQL Dialect"
      override val stringLengthByCharacters: String = "length"
      override def feature_RandomUUID_functionName: String = "gen_random_uuid"
      override def feature_RandomFloat_functionName: Option[String] = Some("random")
      override def feature_RandomFloat_rawSQL: Option[SqlSnippet] = None
      override def feature_RandomInt_rawSQL: SqlSnippet =
        val a = ("a", Precedence.Concat)
        val b = ("b", Precedence.Concat)
        SqlSnippet(
          Precedence.Unary,
          snippet"(with randomIntParameters as (select $a as a, $b as b) select floor(random() * (b - a + 1) + a)::integer from randomIntParameters)"
        )
      override def stringPositionFindingVia: String = "POSITION"
      // override val `prefers $n over ? for parametrization` = false
      override def arrayLengthFunctionName: String = "CARDINALITY"

    given RandomUUID = new RandomUUID {}
    given RandomIntegerInInclusiveRange = new RandomIntegerInInclusiveRange {}
    given INCanHandleRows = new INCanHandleRows {}
    given ReversibleStrings = new ReversibleStrings {}
    given HomogenousArraysOf1D = new HomogenousArraysOf1D {}
    given [T: ResultTag]: CanBeEqualed[T, T] = new CanBeEqualed[T, T] {}
    given [T1: Numeric, T2: Numeric]: CanBeEqualed[T1, T2] = new CanBeEqualed[T1, T2] {}

// ╔════════════════════════════════════════════════════════════════════╗
// ║                          MySQL Dialect                             ║
// ╚════════════════════════════════════════════════════════════════════╝
  object mysql:
    given Dialect = new MySQLDialect
    class MySQLDialect extends Dialect
        with QuotingIdentifiers.MysqlBehavior
        with LimitAndOffset.MysqlLike
        with StringLiteral.MysqlBehavior:
      def name() = "MySQL Dialect"
      override val xorOperatorSupportedNatively = true
      override def feature_RandomUUID_functionName: String = "UUID"
      override def feature_RandomFloat_functionName: Option[String] = Some("rand")
      override def feature_RandomFloat_rawSQL: Option[SqlSnippet] = None
      override def feature_RandomInt_rawSQL: SqlSnippet =
        val a = ("a", Precedence.Concat)
        val b = ("b", Precedence.Concat)
        SqlSnippet(
          Precedence.Unary,
          snippet"(with randomIntParameters as (select $a as a, $b as b) select floor(rand() * (b - a + 1) + a) from randomIntParameters)"
        )
      override val nullSafeEqualityViaSpecialOperator: Boolean = true
      override val booleanCast: String = "SIGNED"
      override val integerCast: String = "DECIMAL"
      override val doubleCast: String = "DOUBLE"
      override val stringCast: String = "CHAR"
      override val longCast: String = "SIGNED INTEGER"

    given RandomUUID = new RandomUUID {}
    given AcceptsLimitInDeletes = new AcceptsLimitInDeletes {}
    given AcceptsOrderByInDeletes = new AcceptsOrderByInDeletes {}
    given AcceptsLimitAndOrderByInUpdates = new AcceptsLimitAndOrderByInUpdates {}
    given INCanHandleRows = new INCanHandleRows {}
    given RandomIntegerInInclusiveRange = new RandomIntegerInInclusiveRange {}
    given ReversibleStrings = new ReversibleStrings {}
    given [T1, T2]: CanBeEqualed[T1, T2] = new CanBeEqualed[T1, T2] {}

// ╔════════════════════════════════════════════════════════════════════╗
// ║                         MariaDB Dialect                            ║
// ╚════════════════════════════════════════════════════════════════════╝
  object mariadb:
    // XXX MariaDB extends MySQL
    // XXX but you still have to redeclare the givens
    given Dialect = new mysql.MySQLDialect with QuotingIdentifiers.MariadbBehavior:
      override def name() = "MariaDB Dialect"

    given RandomUUID = mysql.given_RandomUUID
    given INCanHandleRows = new INCanHandleRows {}
    given AcceptsLimitInDeletes = new AcceptsLimitInDeletes {}
    given AcceptsOrderByInDeletes = new AcceptsOrderByInDeletes {}
    given AcceptsLimitAndOrderByInUpdates = new AcceptsLimitAndOrderByInUpdates {}
    given RandomIntegerInInclusiveRange = mysql.given_RandomIntegerInInclusiveRange
    given ReversibleStrings = mysql.given_ReversibleStrings
    given [T1, T2]: CanBeEqualed[T1, T2] = new CanBeEqualed[T1, T2] {}

// ╔════════════════════════════════════════════════════════════════════╗
// ║                         SQLite Dialect                             ║
// ╚════════════════════════════════════════════════════════════════════╝
  object sqlite:
    given Dialect = new Dialect
      with QuotingIdentifiers.SqliteBehavior
      with LimitAndOffset.Separate
      with StringLiteral.AnsiSingleQuote:
      def name() = "SQLite Dialect"
      override val stringLengthByCharacters = "length"
      override def feature_RandomFloat_functionName: Option[String] = None
      override def feature_RandomFloat_rawSQL: Option[SqlSnippet] =
        Some(SqlSnippet(Precedence.Unary, snippet"(0.5 - RANDOM() / CAST(-9223372036854775808 AS REAL) / 2)"))
      override def feature_RandomInt_rawSQL: SqlSnippet =
        val a = ("a", Precedence.Concat)
        val b = ("b", Precedence.Concat)
        SqlSnippet(
          Precedence.Unary,
          snippet"(with randomIntParameters as (select $a as a, $b as b) select cast(abs(random() % (b - a + 1) + a) as integer) from randomIntParameters)"
        )
      override def needsStringRepeatPolyfill: Boolean = true
      override def needsStringLPadRPadPolyfill: Boolean = true
      override def stringPositionFindingVia: String = "INSTR"
      override def shouldHaveParensInsideValuesExpr: Boolean = false
      override def canExtractDateTimeComponentsNatively: Boolean = false

    given INCanHandleRows = new INCanHandleRows {}
    given RandomIntegerInInclusiveRange = new RandomIntegerInInclusiveRange {}
    given ReversibleStrings = new ReversibleStrings {}
    given [T1, T2]: CanBeEqualed[T1, T2] = new CanBeEqualed[T1, T2] {}

// ╔════════════════════════════════════════════════════════════════════╗
// ║                           H2 Dialect                               ║
// ╚════════════════════════════════════════════════════════════════════╝
  object h2:
    given Dialect = new Dialect
      with QuotingIdentifiers.H2Behavior
      with LimitAndOffset.Separate
      with StringLiteral.AnsiSingleQuote:
      def name() = "H2 Dialect"
      override val stringLengthByCharacters = "length"
      override def feature_RandomUUID_functionName: String = "RANDOM_UUID"
      override def feature_RandomFloat_functionName: Option[String] = Some("rand")
      override def feature_RandomFloat_rawSQL: Option[SqlSnippet] = None
      override def feature_RandomInt_rawSQL: SqlSnippet =
        val a = ("a", Precedence.Concat)
        val b = ("b", Precedence.Concat)
        SqlSnippet(
          Precedence.Unary,
          snippet"(with randomIntParameters as (select $a as a, $b as b) select floor(rand() * (b - a + 1) + a) from randomIntParameters)"
        )
      override def arrayPrependFunctionName: String = "ARRAY_APPEND"

    given INCanHandleRows = new INCanHandleRows {}
    given RandomUUID = new RandomUUID {}
    given RandomIntegerInInclusiveRange = new RandomIntegerInInclusiveRange {}
    given [T: ResultTag]: CanBeEqualed[T, T] = new CanBeEqualed[T, T] {}
    given [T1: Numeric, T2: Numeric]: CanBeEqualed[T1, T2] = new CanBeEqualed[T1, T2] {}
    given HomogenousArraysOf1D = new HomogenousArraysOf1D {}
    given AcceptsLimitInDeletes = new AcceptsLimitInDeletes {}
    given AcceptsLimitAndOrderByInUpdates = new AcceptsLimitAndOrderByInUpdates {}

// ╔════════════════════════════════════════════════════════════════════╗
// ║                         DuckDB Dialect                             ║
// ╚════════════════════════════════════════════════════════════════════╝
  object duckdb:
    given Dialect = new Dialect
      with QuotingIdentifiers.DuckdbBehavior
      with LimitAndOffset.Separate
      with StringLiteral.DuckdbBehavior:
      override def name(): String = "DuckDB Dialect"
      override val stringLengthByCharacters = "length"
      override val stringLengthBytesNeedsEncodeFirst = true
      override def feature_RandomUUID_functionName: String = "uuid"
      override def feature_RandomFloat_functionName: Option[String] = Some("random")
      override def feature_RandomFloat_rawSQL: Option[SqlSnippet] = None
      override def feature_RandomInt_rawSQL: SqlSnippet =
        val a = ("a", Precedence.Concat)
        val b = ("b", Precedence.Concat)
        SqlSnippet(
          Precedence.Unary,
          snippet"(with randomIntParameters as (select $a as a, $b as b) select floor(random() * (b - a + 1) + a)::integer from randomIntParameters)"
        )
      override def stringPositionFindingVia: String = "POSITION"
      override val `prefers $n over ? for parametrization` = true
      override def needsByteByByteEscapingOfBlobs: Boolean = true

    given RandomUUID = new RandomUUID {}
    given RandomIntegerInInclusiveRange = new RandomIntegerInInclusiveRange {}
    given ReversibleStrings = new ReversibleStrings {}
    given HomogenousArraysOf1D = new HomogenousArraysOf1D {}
    given [T: ResultTag]: CanBeEqualed[T, T] = new CanBeEqualed[T, T] {}
    given [T1: Numeric, T2: Numeric]: CanBeEqualed[T1, T2] = new CanBeEqualed[T1, T2] {}

package test.integration.strings

import munit.FunSuite
import test.{withDBNoImplicits, withDB, checkExpr, checkExprDialect}
import java.sql.{Connection, Statement, ResultSet}
import tyql.{Dialect, Table, Expr, lit}
import tyql.Dialect.{literal_percent, literal_underscore}

class StringTests extends FunSuite {
  test("string length by characters and bytes, length is aliased to characterLength tests") {
    def checkValue(expected: Int)(rs: ResultSet) = assertEquals(rs.getInt(1), expected)

    // XXX the expression is defined under ANSI SQL dialect, but toSQLQuery is run against a specific dialect and it works!
    assertEquals(summon[Dialect].name(), "ANSI SQL Dialect")
    val s = lit("ałajć")
    checkExprDialect[Int](s.length, checkValue(5))(withDB.all)
    checkExprDialect[Int](s.charLength, checkValue(5))(withDB.all)
    checkExprDialect[Int](s.byteLength, checkValue(7))(withDB.all)
  }

  test("upper and lower work also with unicode") {
    def checkValue(expected: String)(rs: ResultSet) = assertEquals(rs.getString(1), expected)

    for (
      r <- Seq(
        withDBNoImplicits.postgres[Unit],
        withDBNoImplicits.mariadb[Unit],
        withDBNoImplicits.mysql[Unit],
        withDBNoImplicits.h2[Unit],
        withDBNoImplicits.duckdb[Unit]
      )
    ) {
      checkExpr[String](lit("aŁaJć").toUpperCase, checkValue("AŁAJĆ"))(r)
      checkExpr[String](lit("aŁaJć").toLowerCase, checkValue("ałajć"))(r)
    }

    // SQLite does not support unicode case folding by default unless compiled with ICU support
    checkExpr[String](lit("A bRoWn fOX").toUpperCase, checkValue("A BROWN FOX"))(withDBNoImplicits.sqlite)
    checkExpr[String](lit("A bRoWn fOX").toLowerCase, checkValue("a brown fox"))(withDBNoImplicits.sqlite)
  }

  test("trim ltrim rtrim") {
    checkExprDialect[String](
      lit("  a  b  c  ").trim,
      (rs: ResultSet) => assertEquals(rs.getString(1), "a  b  c")
    )(withDB.all)
    checkExprDialect[String](
      lit("  a  b  c  ").ltrim,
      (rs: ResultSet) => assertEquals(rs.getString(1), "a  b  c  ")
    )(withDB.all)
    checkExprDialect[String](
      lit("  a  b  c  ").rtrim,
      (rs: ResultSet) => assertEquals(rs.getString(1), "  a  b  c")
    )(withDB.all)
    checkExprDialect[String](
      lit("  a  b  c  ").stripLeading,
      (rs: ResultSet) => assertEquals(rs.getString(1), "a  b  c  ")
    )(withDB.all)
    checkExprDialect[String](
      lit("  a  b  c  ").stripTrailing,
      (rs: ResultSet) => assertEquals(rs.getString(1), "  a  b  c")
    )(withDB.all)
  }

  test("string replace") {
    checkExprDialect[String](
      lit("aabbccaaa").replace(lit("aa"), lit("XX")),
      (rs: ResultSet) => assertEquals(rs.getString(1), "XXbbccXXa")
    )(withDB.all)
  }

  test("substr and unicode") {
    checkExprDialect[String](
      lit("ałajć").substr(lit(2), lit(1)),
      (rs: ResultSet) => assertEquals(rs.getString(1), "ł")
    )(withDB.all)
    checkExprDialect[String](
      lit("ałajć").substr(lit(2)),
      (rs: ResultSet) => assertEquals(rs.getString(1), "łajć")
    )(withDB.all)
  }

  test("substr vs substring") {
    val s = lit("012345")

    checkExprDialect[String](
      s.substr(lit(1), lit(1)),
      (rs: ResultSet) => assertEquals(rs.getString(1), "0"),
    )(withDB.all)
    checkExprDialect[String](
      s.substr(lit(4), lit(2)),
      (rs: ResultSet) => assertEquals(rs.getString(1), "34")
    )(withDB.all)
    checkExprDialect[String](
      s.substr(lit(4), lit(3)),
      (rs: ResultSet) => assertEquals(rs.getString(1), "345")
    )(withDB.all)

    checkExprDialect[String](
      s.substring(lit(0), lit(1)),
      (rs: ResultSet) => assertEquals(rs.getString(1), "0")
    )(withDB.all)
    checkExprDialect[String](
      s.substring(lit(3), lit(5)),
      (rs: ResultSet) => assertEquals(rs.getString(1), "34")
    )(withDB.all)
    checkExprDialect[String](
      s.substring(lit(3), lit(6)),
      (rs: ResultSet) => assertEquals(rs.getString(1), "345")
    )(withDB.all)
  }

  test("LIKE patterns") {
    def checkValue(expected: Boolean)(rs: ResultSet) = assertEquals(rs.getBoolean(1), expected)

    checkExprDialect[Boolean](
      lit("abba").like(lit("abba")),
      checkValue(true)
    )(withDB.all)

    checkExprDialect[Boolean](
      lit("abba").like(lit("a_b_")),
      checkValue(true)
    )(withDB.all)

    checkExprDialect[Boolean](
      lit("abba").like(lit("bba")),
      checkValue(false)
    )(withDB.all)

    checkExprDialect[Boolean](
      lit("abba").like(lit("a%")),
      checkValue(true)
    )(withDB.all)

    checkExprDialect[Boolean](
      lit("abba").like(lit("%")),
      checkValue(true)
    )(withDB.all)

    checkExprDialect[Boolean](
      lit("abba").like(lit("___")),
      checkValue(false)
    )(withDB.all)
  }

  test("LIKE patterns handle % and _ differently") {
    def checkValue(expected: Boolean)(rs: ResultSet) = assertEquals(rs.getBoolean(1), expected)

    checkExprDialect[Boolean](
      lit("a%bc").like(lit("a%")),
      checkValue(true)
    )(withDB.all)
    checkExprDialect[Boolean](
      lit("a%bc").like(lit("a" + literal_percent)),
      checkValue(false)
    )(withDB.all)
    checkExprDialect[Boolean](
      lit("a%bc").like(lit("a" + literal_percent + "bc")),
      checkValue(true)
    )(withDB.all)
    checkExprDialect[Boolean](
      lit("ab").like(lit("_b")),
      checkValue(true)
    )(withDB.all)
    checkExprDialect[Boolean](
      lit("ab").like(lit(literal_underscore + "b")),
      checkValue(false)
    )(withDB.all)
    checkExprDialect[Boolean](
      lit("_b").like(lit(literal_underscore + "b")),
      checkValue(true)
    )(withDB.all)
  }

  test("concatenation of two strings") {
    def checkValue(expected: String)(rs: ResultSet) = assertEquals(rs.getString(1), expected)

    checkExprDialect[String](
      lit("__a") + lit("bcd"),
      checkValue("__abcd")
    )(withDB.all)

    checkExprDialect[String](
      lit("__a") + (lit("bc") + lit("d")),
      checkValue("__abcd")
    )(withDB.all)

    checkExprDialect[String](
      (lit("__") + lit("a")) + lit("bcd"),
      checkValue("__abcd")
    )(withDB.all)
  }

  test("concatenation of multiple strings") {
    def checkValue(expected: String)(rs: ResultSet) = assertEquals(rs.getString(1), expected)

    checkExprDialect[String](
      Expr.concat(Seq(lit("a"), lit("b"), lit("CCCC"))),
      checkValue("abCCCC")
    )(withDB.all)

    checkExprDialect[String](
      Expr.concatWith(Seq(lit("a"), lit("b"), lit("CCCC")), lit("--")),
      checkValue("a--b--CCCC")
    )(withDB.all)
  }

  test("reverse") {
    def checkValue(expected: String)(rs: ResultSet) = assertEquals(rs.getString(1), expected)
    import tyql.DialectFeature.ReversibleStrings
    given ReversibleStrings = new ReversibleStrings {}

    checkExprDialect[String](lit("aBcł").reverse, checkValue("łcBa"))(withDB.postgres)
    checkExprDialect[String](lit("aBcł").reverse, checkValue("łcBa"))(withDB.sqlite)
    checkExprDialect[String](lit("aBcł").reverse, checkValue("łcBa"))(withDB.duckdb)
    checkExprDialect[String](lit("aBcł").reverse, checkValue("łcBa"))(withDB.mysql)
    checkExprDialect[String](lit("aBcł").reverse, checkValue("łcBa"))(withDB.mariadb)
  }

  test("repeat") {
    def checkValue(expected: String)(rs: ResultSet) = assertEquals(rs.getString(1), expected)
    checkExprDialect[String](lit("aB").repeat(lit(3)), checkValue("aBaBaB"))(withDB.all)
    checkExprDialect[String](lit("aB").repeat(lit(1)), checkValue("aB"))(withDB.all)
    checkExprDialect[String](lit("aB").repeat(lit(0)), checkValue(""))(withDB.all)
  }

  test("lpad rpad") {
    def checkValue(expected: String)(rs: ResultSet) = assertEquals(rs.getString(1), expected)
    checkExprDialect[String](lit("1234").lpad(lit(7), lit("ZZ")), checkValue("ZZZ1234"))(withDB.all)
    checkExprDialect[String](lit("1234").rpad(lit(7), lit("ZZ")), checkValue("1234ZZZ"))(withDB.all)

    checkExprDialect[String](lit("1234").lpad(lit(4), lit("ZZ")), checkValue("1234"))(withDB.all)
    checkExprDialect[String](lit("1234").rpad(lit(4), lit("ZZ")), checkValue("1234"))(withDB.all)

    checkExprDialect[String](lit("1234").lpad(lit(2), lit("ZZ")), checkValue("12"))(withDB.all)
    checkExprDialect[String](lit("1234").rpad(lit(2), lit("ZZ")), checkValue("12"))(withDB.all)
    checkExprDialect[String](lit("1234").lpad(lit(0), lit("ZZ")), checkValue(""))(withDB.all)
    checkExprDialect[String](lit("1234").rpad(lit(0), lit("ZZ")), checkValue(""))(withDB.all)

    // -1 will give you '' or null depending on the DB
  }

  test("string position") {
    def checkValue(expected: Int)(rs: ResultSet) = assertEquals(rs.getInt(1), expected)
    checkExprDialect[Int](lit("aBc").findPosition(lit("B")), checkValue(2))(withDB.all)
    // TODO XXX now the case sensitivity is not handled at all and is DB-specific!
    for (r <- Seq(withDB.postgres[Unit], withDB.h2[Unit], withDB.duckdb[Unit], withDB.sqlite[Unit])) {
      checkExprDialect[Int](lit("abba aBba ABba").findPosition(lit("Bb")), checkValue(7))(r)
    }
    for (r <- Seq(withDB.mysql[Unit], withDB.mariadb[Unit])) {
      checkExprDialect[Int](lit("abba aBba ABba").findPosition(lit("Bb")), checkValue(2))(r)
    }
  }
}

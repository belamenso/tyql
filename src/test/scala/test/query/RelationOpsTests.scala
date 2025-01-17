package test.query.relationops
import test.SQLStringQueryTest
import test.query.{commerceDBs, AllCommerceDBs, Product}

import tyql.*
import tyql.Expr.toRow
import language.experimental.namedTuples
import NamedTuple.*
import scala.language.implicitConversions

class RelationOpsUnionTest extends SQLStringQueryTest[AllCommerceDBs, (id: Int)] {
  def testDescription: String = "RelationOps: union with project"
  def query() =
    testDB.tables.products
      .map: prod =>
        (id = prod.id).toRow
      .union(testDB.tables.purchases
        .map: purch =>
          (id = purch.id).toRow)
  def expectedQueryPattern: String = """
        (SELECT product$A.id as id
        FROM product as product$A)
        UNION
        (SELECT purchase$B.id as id
        FROM purchase as purchase$B)
      """
}

class RelationOpsUnion2Test extends SQLStringQueryTest[AllCommerceDBs, Product] {
  def testDescription: String = "RelationOps: union"
  def query() =
    testDB.tables.products
      .map(prod => prod)
      .union(testDB.tables.products.map(purch => purch))
  def expectedQueryPattern: String = """
        (SELECT product$A.*
        FROM product as product$A)
        UNION
        (SELECT product$B.*
        FROM product as product$B)
      """
}

class RelationOpsUnion3Test extends SQLStringQueryTest[AllCommerceDBs, Product] {
  def testDescription: String = "RelationOps: union n-ary"

  def query() =
    testDB.tables.products
      .map(prod => prod)
      .union(testDB.tables.products.map(purch => purch))
      .union(testDB.tables.products.map(purch => purch))

  def expectedQueryPattern: String =
    """
          (SELECT product$A.*
          FROM product as product$A)
          UNION
          (SELECT product$B.*
          FROM product as product$B)
          UNION
          (SELECT product$C.*
          FROM product as product$C)
        """
}

class RelationOpsUnion4Test extends SQLStringQueryTest[AllCommerceDBs, Product] {
  def testDescription: String = "RelationOps: union n-ary"

  def query() =
    testDB.tables.products.map(prod => prod)
      .union(testDB.tables.products.map(prod => prod))
      .union(testDB.tables.products.map(prod => prod))
      .union(testDB.tables.products.map(prod => prod))

  def expectedQueryPattern: String =
    """
            (SELECT product$A.*
            FROM product as product$A)
            UNION
            (SELECT product$B.*
            FROM product as product$B)
            UNION
            (SELECT product$C.*
            FROM product as product$C)
            UNION
            (SELECT product$D.*
            FROM product as product$D)
          """
}

class RelationOpsUnion5Test extends SQLStringQueryTest[AllCommerceDBs, Product] {
  def testDescription: String = "RelationOps: n-ary with nested unions get flattened"

  def query() =
    testDB.tables.products.map(prod => prod)
      .union(
        testDB.tables.products.map(prod => prod)
          .union(testDB.tables.products.map(prod => prod))
      ).union(testDB.tables.products.map(prod => prod))

  def expectedQueryPattern: String =
    """
              (SELECT product$A.*
              FROM product as product$A)
              UNION
              (SELECT product$B.*
              FROM product as product$B)
              UNION
              (SELECT product$C.*
              FROM product as product$C)
              UNION
              (SELECT product$D.*
              FROM product as product$D)
            """
}

class RelationOpsUnionMixedTest extends SQLStringQueryTest[AllCommerceDBs, (id: Int)] {
  def testDescription: String = "RelationOps: n-ary with nested unions + other op do not get flattened"

  def query() =
    testDB.tables.products.map(prod => (id = prod.id))
      .union(
        testDB.tables.buyers.map(buyer => (id = buyer.id))
          .unionAll(testDB.tables.purchases.map(pr => (id = pr.id)))
      )

  def expectedQueryPattern: String =
    """
                (SELECT product$A.id as id
                FROM product as product$A)
                UNION
                ((SELECT buyers$B.id as id
                FROM buyers as buyers$B)
                UNION ALL
                (SELECT purchase$C.id as id
                FROM purchase as purchase$C))
              """
}

class RelationOpsUnionMixedDistinctTest extends SQLStringQueryTest[AllCommerceDBs, (id: Int)] {
  def testDescription: String = "RelationOps: n-ary with nested unions + other op do not get flattened with distinct"

  def query() =
    testDB.tables.products.map(prod => (id = prod.id))
      .union(
        testDB.tables.buyers.map(buyer => (id = buyer.id))
          .unionAll(testDB.tables.purchases.map(pr => (id = pr.id))).distinct
      )

  def expectedQueryPattern: String =
    """
                  (SELECT product$A.id as id
                  FROM product as product$A)
                  UNION
                  (SELECT DISTINCT * FROM ((SELECT buyers$B.id as id
                  FROM buyers as buyers$B)
                  UNION ALL
                  (SELECT purchase$C.id as id
                  FROM purchase as purchase$C)) as subquery$D)
                """
}

class RelationOpsUnionAllTest extends SQLStringQueryTest[AllCommerceDBs, (id: Int)] {
  def testDescription: String = "RelationOps: unionAll with project"
  def query() =
    testDB.tables.products
      .map: prod =>
        (id = prod.id).toRow
      .unionAll(testDB.tables.purchases
        .map: purch =>
          (id = purch.id).toRow)
  def expectedQueryPattern: String = """
        (SELECT product$A.id as id
        FROM product as product$A)
        UNION ALL
        (SELECT purchase$B.id as id
        FROM purchase as purchase$B)
      """
}

class RelationOpsIntersectTest extends SQLStringQueryTest[AllCommerceDBs, (id: Int)] {
  def testDescription: String = "RelationOps: intersect with project"
  def query() =
    testDB.tables.products
      .map: prod =>
        (id = prod.id).toRow
      .intersect(testDB.tables.purchases
        .map: purch =>
          (id = purch.id).toRow)
  def expectedQueryPattern: String = """
        (SELECT product$A.id as id
        FROM product as product$A)
        INTERSECT
        (SELECT purchase$B.id as id
        FROM purchase as purchase$B)
      """
}

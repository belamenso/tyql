package tyql

trait DialectFeature

object DialectFeature:
  trait RandomUUID extends DialectFeature
  trait RandomIntegerInInclusiveRange extends DialectFeature

  trait ReversibleStrings extends DialectFeature

  trait INCanHandleRows extends DialectFeature

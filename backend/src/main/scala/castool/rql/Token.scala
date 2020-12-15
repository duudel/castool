package castool.rql

sealed trait Token extends Serializable with Product {
  def pos: Int
  def display: String
}

object Token {
  sealed abstract class Tok(val display: String) extends Token
  // identifiers
  final case class Ident(value: String, pos: Int) extends Tok(value)
  // literals
  final case class StringLit(value: String, pos: Int) extends Tok("\"" + value + "\"")
  final case class NumberLit(value: Double, pos: Int) extends Tok(s"$value") // TODO: support integer and float numbers
  final case class DateLit(value: String, pos: Int) extends Tok(value)
  final case class NullLit(pos: Int) extends Tok("null")
  final case class TrueLit(pos: Int) extends Tok("true")
  final case class FalseLit(pos: Int) extends Tok("false")
  // special
  final case class Bar(pos: Int) extends Tok("|")
  final case class LParen(pos: Int) extends Tok("(")
  final case class RParen(pos: Int) extends Tok(")")
  final case class Comma(pos: Int) extends Tok(",")

  // operators
  sealed trait UnaryOpToken extends Token { def unaryOp: UnaryOp }
  sealed abstract class UnOpToken(val unaryOp: UnaryOp) extends UnaryOpToken { def display: String = unaryOp.display }
  sealed abstract class BinOpToken(val binaryOp: BinaryOp) extends Token { def display: String = binaryOp.display }
  //sealed abstract class UnBinOpToken(binOp: BinaryOp, val unaryOp: UnaryOp) extends BinOpToken(binOp) with UnaryOpToken
  sealed abstract class AssignOp(op: BinaryOp.AssignOp) extends BinOpToken(op)
  final case class OpAssign(pos: Int) extends AssignOp(BinaryOp.Assign)

  final case class OpNot(pos: Int) extends UnOpToken(UnaryOp.Not)

  sealed abstract class ArithmeticOp(op: BinaryOp.ArithmeticOp) extends BinOpToken(op)
  final case class OpPlus(pos: Int) extends ArithmeticOp(BinaryOp.Plus) with UnaryOpToken { def unaryOp: UnaryOp = UnaryOp.Plus }
  final case class OpMinus(pos: Int) extends ArithmeticOp(BinaryOp.Minus) with UnaryOpToken { def unaryOp: UnaryOp = UnaryOp.Minus }
  final case class OpMultiply(pos: Int) extends ArithmeticOp(BinaryOp.Multiply)
  final case class OpDivide(pos: Int) extends ArithmeticOp(BinaryOp.Divide)

  sealed abstract class ComparisonOp(op: BinaryOp.CompareOp) extends BinOpToken(op)
  final case class OpEqual(pos: Int) extends ComparisonOp(BinaryOp.Equal)
  final case class OpNotEqual(pos: Int) extends ComparisonOp(BinaryOp.NotEqual)
  final case class OpLess(pos: Int) extends ComparisonOp(BinaryOp.Less)
  final case class OpLessEq(pos: Int) extends ComparisonOp(BinaryOp.LessEq)
  final case class OpGreater(pos: Int) extends ComparisonOp(BinaryOp.Greater)
  final case class OpGreaterEq(pos: Int) extends ComparisonOp(BinaryOp.GreaterEq)

  sealed abstract class StringOp(op: BinaryOp.StringOp) extends BinOpToken(op)
  final case class OpContains(pos: Int) extends StringOp(BinaryOp.Contains)
  final case class OpNotContains(pos: Int) extends StringOp(BinaryOp.NotContains)
  
  sealed abstract class LogicalOp(op: BinaryOp.LogicalOp) extends BinOpToken(op)
  final case class OpAnd(pos: Int) extends LogicalOp(BinaryOp.And)
  final case class OpOr(pos: Int) extends LogicalOp(BinaryOp.Or)
}


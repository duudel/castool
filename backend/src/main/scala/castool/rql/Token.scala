package castool.rql

import TokenKind.TokenKind

sealed trait Token extends Serializable with Product {
  def pos: Int
  def display: String
  def kind: TokenKind[_]
}

object Token {
  sealed abstract class Tok(val kind: TokenKind[_]) extends Token { def display: String = kind.print }
  // identifiers
  final case class Ident(value: String, pos: Int) extends Tok(TokenKind.ident)
  // literals
  final case class StringLit(value: String, pos: Int) extends Tok(TokenKind.string)
  final case class NumberLit(value: Double, pos: Int) extends Tok(TokenKind.number) // TODO: support integer and float numbers
  final case class DateLit(value: Date, pos: Int) extends Tok(TokenKind.date)
  final case class NullLit(pos: Int) extends Tok(TokenKind.nullKind)
  final case class TrueLit(pos: Int) extends Tok(TokenKind.trueKind)
  final case class FalseLit(pos: Int) extends Tok(TokenKind.falseKind)
  // special
  final case class Bar(pos: Int) extends Tok(TokenKind.bar)
  final case class LParen(pos: Int) extends Tok(TokenKind.lparen)
  final case class RParen(pos: Int) extends Tok(TokenKind.rparen)
  final case class Comma(pos: Int) extends Tok(TokenKind.comma)

  // operators
  final case class OpAssign(pos: Int) extends Tok(TokenKind.assign)

  sealed trait UnaryOpToken extends Token { def unaryOp: UnaryOp }
  sealed abstract class UnOpToken(val unaryOp: UnaryOp, val kind: TokenKind[UnOpToken]) extends UnaryOpToken { def display: String = unaryOp.display; }
  sealed abstract class BinOpToken(val binaryOp: BinaryOp) extends Token { def display: String = binaryOp.display }
  //sealed abstract class UnBinOpToken(binOp: BinaryOp, val unaryOp: UnaryOp) extends BinOpToken(binOp) with UnaryOpToken

  final case class OpNot(pos: Int) extends UnOpToken(UnaryOp.Not, TokenKind.not)

  sealed abstract class ArithmeticOp(op: BinaryOp.ArithmeticOp, val kind: TokenKind[ArithmeticOp]) extends BinOpToken(op)
  final case class OpPlus(pos: Int) extends ArithmeticOp(BinaryOp.Plus, TokenKind.plus) with UnaryOpToken { def unaryOp: UnaryOp = UnaryOp.Plus }
  final case class OpMinus(pos: Int) extends ArithmeticOp(BinaryOp.Minus, TokenKind.minus) with UnaryOpToken { def unaryOp: UnaryOp = UnaryOp.Minus }
  final case class OpMultiply(pos: Int) extends ArithmeticOp(BinaryOp.Multiply, TokenKind.multiply)
  final case class OpDivide(pos: Int) extends ArithmeticOp(BinaryOp.Divide, TokenKind.divide)

  sealed abstract class ComparisonOp(op: BinaryOp.CompareOp, val kind: TokenKind[ComparisonOp]) extends BinOpToken(op)
  final case class OpEqual(pos: Int) extends ComparisonOp(BinaryOp.Equal, TokenKind.eq)
  final case class OpNotEqual(pos: Int) extends ComparisonOp(BinaryOp.NotEqual, TokenKind.notEq)
  final case class OpLess(pos: Int) extends ComparisonOp(BinaryOp.Less, TokenKind.less)
  final case class OpLessEq(pos: Int) extends ComparisonOp(BinaryOp.LessEq, TokenKind.lessEq)
  final case class OpGreater(pos: Int) extends ComparisonOp(BinaryOp.Greater, TokenKind.greater)
  final case class OpGreaterEq(pos: Int) extends ComparisonOp(BinaryOp.GreaterEq, TokenKind.greaterEq)

  sealed abstract class StringOp(op: BinaryOp.StringOp, val kind: TokenKind[StringOp]) extends BinOpToken(op)
  final case class OpContains(pos: Int) extends StringOp(BinaryOp.Contains, TokenKind.contains)
  final case class OpNotContains(pos: Int) extends StringOp(BinaryOp.NotContains, TokenKind.notContains)

  sealed abstract class LogicalOp(op: BinaryOp.LogicalOp, val kind: TokenKind[LogicalOp]) extends BinOpToken(op)
  final case class OpAnd(pos: Int) extends LogicalOp(BinaryOp.And, TokenKind.and)
  final case class OpOr(pos: Int) extends LogicalOp(BinaryOp.Or, TokenKind.or)
}

object TokenKind {
  import Token._
  trait TokenKind[+T <: Token] {
    def print: String
  }

  def print[T <: Token: TokenKind] = implicitly[TokenKind[T]].print
  def kind[T <: Token: TokenKind] = implicitly[TokenKind[T]]

  implicit val ident = new TokenKind[Ident] { def print: String = "<identifier>" }
  implicit val string = new TokenKind[StringLit] { def print: String = "<string-literal>" }
  implicit val number = new TokenKind[NumberLit] { def print: String = "<number-literal>" }
  implicit val date = new TokenKind[DateLit] { def print: String = "<date-literal>" }
  implicit val nullKind = new TokenKind[NullLit] { def print: String = "null" }
  implicit val trueKind = new TokenKind[TrueLit] { def print: String = "true" }
  implicit val falseKind = new TokenKind[FalseLit] { def print: String = "false" }
  // special
  implicit val bar = new TokenKind[Bar] { def print: String = "|" }
  implicit val lparen = new TokenKind[LParen] { def print: String = "(" }
  implicit val rparen = new TokenKind[RParen]  { def print: String = ")" }
  implicit val comma = new TokenKind[Comma] { def print: String = "," }

  def binOp[T <: BinOpToken](op: BinaryOp): TokenKind[T] = new TokenKind[T] {
    def print: String = op.display
  }

  implicit val not: TokenKind[OpNot] = new TokenKind[OpNot] {
    def print: String = UnaryOp.Not.display
  }

  implicit val assign: TokenKind[OpAssign] = new TokenKind[OpAssign] { def print: String = "=" }
  implicit val plus: TokenKind[OpPlus] = binOp(BinaryOp.Plus)
  implicit val minus: TokenKind[OpMinus] = binOp(BinaryOp.Minus)
  implicit val multiply: TokenKind[OpMultiply] = binOp(BinaryOp.Multiply)
  implicit val divide: TokenKind[OpDivide] = binOp(BinaryOp.Divide)
  implicit val eq: TokenKind[OpEqual] = binOp(BinaryOp.Equal)
  implicit val notEq: TokenKind[OpNotEqual] = binOp(BinaryOp.NotEqual)
  implicit val less: TokenKind[OpLess] = binOp(BinaryOp.Less)
  implicit val lessEq: TokenKind[OpLessEq] = binOp(BinaryOp.LessEq)
  implicit val greater: TokenKind[OpGreater] = binOp(BinaryOp.Greater)
  implicit val greaterEq: TokenKind[OpGreaterEq] = binOp(BinaryOp.GreaterEq)
  implicit val contains: TokenKind[OpContains] = binOp(BinaryOp.Contains)
  implicit val notContains: TokenKind[OpNotContains] = binOp(BinaryOp.NotContains)
  implicit val and: TokenKind[OpAnd] = binOp(BinaryOp.And)
  implicit val or: TokenKind[OpOr] = binOp(BinaryOp.Or)

}


package castool.rql

sealed trait Ast extends Serializable with Product {
  def pos: Int
}

object Ast {
  sealed trait Source extends Ast
  final case class Table(name: Token, pos: Int) extends Source
  final case class Cont(source: Source, op: TopLevelOp, pos: Int) extends Source

  sealed trait TopLevelOp extends Ast with Serializable with Product
  final case class Where(expr: Expr, pos: Int) extends TopLevelOp
  final case class Project(names: Seq[Token.Ident], pos: Int) extends TopLevelOp
  final case class Extend(name: Token.Ident, expr: Expr, pos: Int) extends TopLevelOp
  final case class OrderBy(names: Seq[Token.Ident], order: Order, pos: Int) extends TopLevelOp

  case class Aggregation(name: Token.Ident, aggr: FunctionCall)
  final case class Summarize(aggregations: Seq[Aggregation], groupBy: Seq[Token.Ident], pos: Int) extends TopLevelOp

  sealed trait Expr extends Ast

  final case class Column(name: Token.Ident, pos: Int) extends Expr
  final case class NullLit(pos: Int) extends Expr
  final case class TrueLit(pos: Int) extends Expr
  final case class FalseLit(pos: Int) extends Expr
  final case class StringLit(value: Token.StringLit, pos: Int) extends Expr
  final case class NumberLit(value: Token.NumberLit, pos: Int) extends Expr
  //final case class DateLit(value: Token.DateLit, pos: Int) extends Expr

  final case class UnaryExpr(op: UnaryOp, expr: Expr, pos: Int) extends Expr
  final case class BinaryExpr(op: BinaryOp, exprA: Expr, exprB: Expr, pos: Int) extends Expr
  final case class FunctionCall(functionName: Token.Ident, args: Seq[Expr], pos: Int) extends Expr

}


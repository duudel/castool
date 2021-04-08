package castool.rql

sealed trait Ast extends Serializable with Product {
  def pos: Int
}
  
case class NameAndToken(name: Name, tok: Token.Ident)

object Ast {

  sealed trait Source extends Ast
  final case class Table(name: Name, tok: Token) extends Source { def pos: Int = tok.pos }
  final case class Cont(source: Source, op: TopLevelOp) extends Source { def pos: Int = op.pos }

  sealed trait TopLevelOp extends Ast with Serializable with Product
  final case class Where(expr: Expr, pos: Int) extends TopLevelOp
  final case class Project(names: Seq[NameAndToken], pos: Int) extends TopLevelOp
  final case class Extend(name: NameAndToken, expr: Expr, pos: Int) extends TopLevelOp
  final case class OrderBy(names: Seq[NameAndToken], order: Order, pos: Int) extends TopLevelOp

  case class Aggregation(name: NameAndToken, aggr: FunctionCall)
  final case class Summarize(aggregations: Seq[Aggregation], groupBy: Seq[NameAndToken], pos: Int) extends TopLevelOp

  sealed trait Expr extends Ast

  final case class Column(name: Name, pos: Int) extends Expr
  final case class NullLit(pos: Int) extends Expr
  final case class TrueLit(pos: Int) extends Expr
  final case class FalseLit(pos: Int) extends Expr
  final case class StringLit(value: Token.StringLit) extends Expr { def pos: Int = value.pos }
  final case class NumberLit(value: Token.NumberLit) extends Expr { def pos: Int = value.pos }
  //final case class DateLit(value: Token.DateLit, pos: Int) extends Expr

  final case class UnaryExpr(op: UnaryOp, expr: Expr, pos: Int) extends Expr
  final case class BinaryExpr(op: BinaryOp, exprA: Expr, exprB: Expr, pos: Int) extends Expr
  final case class FunctionCall(functionName: Name, args: Seq[Expr], pos: Int) extends Expr

}


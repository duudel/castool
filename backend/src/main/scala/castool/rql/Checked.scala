package castool.rql

sealed trait Checked extends Serializable with Product

object Checked {
  sealed trait Source extends Checked { def sourceDef: SourceDef }
  final case class Table(name: Name, sourceDef: SourceDef) extends Source
  final case class Cont(source: Source, op: TopLevelOp) extends Source { def sourceDef: SourceDef = op.sourceDef }

  sealed trait TopLevelOp extends Checked with Serializable with Product { def sourceDef: SourceDef }
  final case class Where(expr: Expr[Bool], sourceDef: SourceDef) extends TopLevelOp
  final case class Project(assignments: Seq[Assignment], sourceDef: SourceDef) extends TopLevelOp
  final case class Extend(assign: Assignment, sourceDef: SourceDef) extends TopLevelOp
  final case class OrderBy(names: Seq[Name], order: Order, sourceDef: SourceDef) extends TopLevelOp

  case class Aggregation(name: Name, aggr: AggregationCall[Value])
  final case class AggregationCall[A <: Value](aggrDef: AggregationDef[A], args: Seq[Expr[_ <: Value]]) {
    def resultType: ValueType = aggrDef.returnType
  }
  final case class Summarize(aggregations: Seq[Aggregation], groupBy: Seq[Assignment], sourceDef: SourceDef) extends TopLevelOp

  final case class Assignment(name: Name, expr: Expr[Value]) { def resultType: ValueType = expr.resultType }

  sealed trait Expr[+A <: Value] { def resultType: ValueType }
  final case class Column[A <: Value](name: Name, resultType: ValueType) extends Expr[A]
  final case object NullLit extends Expr[Value] { def resultType: ValueType = ValueType.Null }
  final case object TrueLit extends Expr[Bool] { def resultType: ValueType = ValueType.Bool }
  final case object FalseLit extends Expr[Bool] { def resultType: ValueType = ValueType.Bool }
  final case class StringLit(value: String) extends Expr[Str] { def resultType: ValueType = ValueType.Str }
  final case class NumberLit(value: Double) extends Expr[Num] { def resultType: ValueType = ValueType.Num }
  final case class DateLit(value: Date) extends Expr[Date] { def resultType: ValueType = ValueType.Date }

  final case class UnaryExpr[A <: Value](op: UnaryOp, expr: Expr[A]) extends Expr[A] {
    def resultType: ValueType = expr.resultType
  }
  final case class BinaryExpr[A <: Value](op: BinaryOp, exprA: Expr[A], exprB: Expr[A], resultType: ValueType) extends Expr[A]
  final case class FunctionCall[A <: Value](functionDef: FunctionDef[A], args: Seq[Expr[_ <: Value]]) extends Expr[A] {
    def resultType: ValueType = functionDef.returnType
  }

}



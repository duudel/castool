package castool.rql

import scala.collection.SeqMap
import scala.collection.mutable.Buffer
import zio._

import ResolveValueType._

sealed trait Checked extends Serializable with Product

object Checked {
  sealed trait Source extends Checked { def sourceDef: SourceDef }
  final case class Table(name: Name, sourceDef: SourceDef) extends Source
  final case class Cont(source: Source, op: TopLevelOp) extends Source { def sourceDef: SourceDef = op.sourceDef }

  sealed trait TopLevelOp extends Checked with Serializable with Product { def sourceDef: SourceDef }
  final case class Where(expr: Expr[Bool], sourceDef: SourceDef) extends TopLevelOp
  final case class Project(names: Seq[Name], sourceDef: SourceDef) extends TopLevelOp
  final case class Extend(name: Name, expr: Expr[_ <: Value], sourceDef: SourceDef) extends TopLevelOp
  final case class OrderBy(names: Seq[Name], order: Order, sourceDef: SourceDef) extends TopLevelOp

  case class Aggregation(name: Name, aggr: AggregationCall[Value])
  final case class AggregationCall[A <: Value](aggrDef: AggregationDef[A], args: Seq[Expr[_ <: Value]]) {
    def resultType: ValueType = aggrDef.returnType
  }
  final case class Summarize(aggregations: Seq[Aggregation], groupBy: Seq[Name], sourceDef: SourceDef) extends TopLevelOp

  sealed trait Expr[+A <: Value] { def resultType: ValueType }
  final case class Column[A <: Value](name: Name, resultType: ValueType) extends Expr[A]
  final case object NullLit extends Expr[Value] { def resultType: ValueType = ValueType.Null }
  final case object TrueLit extends Expr[Bool] { def resultType: ValueType = ValueType.Bool }
  final case object FalseLit extends Expr[Bool] { def resultType: ValueType = ValueType.Bool }
  final case class StringLit(value: String) extends Expr[Str] { def resultType: ValueType = ValueType.Str }
  final case class NumberLit(value: Double) extends Expr[Num] { def resultType: ValueType = ValueType.Num }
  //final case class DateLit(value: Token.DateLit) extends Expr[A]

  final case class UnaryExpr[A <: Value](op: UnaryOp, expr: Expr[A]) extends Expr[A] {
    def resultType: ValueType = expr.resultType
  }
  final case class BinaryExpr[A <: Value](op: BinaryOp, exprA: Expr[A], exprB: Expr[A], resultType: ValueType) extends Expr[A]
  final case class FunctionCall[A <: Value](functionDef: FunctionDef[A], args: Seq[Expr[_ <: Value]]) extends Expr[A] {
    def resultType: ValueType = functionDef.returnType
  }

}

object SemCheck {
  case class Error(msg: String, loc: Location)
  type Result[+A] = ZIO[Env, Error, A]

  trait Env {
    def tableDef(name: Name): Option[SourceDef]
    def functionDef(name: Name): Option[FunctionDef[Value]]
    def aggregationDef(name: Name): Option[AggregationDef[Value]]
  }

  def check(ast: Ast.Source): Result[Checked.Source] = {
    ast match {
      case Ast.Table(name, tok) =>
        for {
          tableDef <- ZIO.accessM[Env] { env =>
            ZIO.fromOption(env.tableDef(name))
              .mapError(_ => Error(s"No such table as '${name.n}' found", Location(tok.pos)))
          }
        } yield Checked.Table(name, tableDef)
      case Ast.Cont(source, op) =>
        for {
          checkedSource <- check(source)
          checkedOp <- checkOp(op, checkedSource)
        } yield Checked.Cont(checkedSource, checkedOp)
    }
  }

  def checkOp(ast: Ast.TopLevelOp, source: Checked.Source): Result[Checked.TopLevelOp] = {
    val sourceDef = source.sourceDef
    ast match {
      case Ast.Where(expr, pos) =>
        for {
          checkedExpr <- checkTypedExpr[Bool](expr, sourceDef)
        } yield Checked.Where(checkedExpr, sourceDef)
      case Ast.Project(nameAndTok, pos) =>
        val projected = nameAndTok.map {
          case NameAndToken(name, tok) => name -> (sourceDef.get(name) -> tok)
        }
        val errors = projected.collect {
          case (name, (None, tok)) => (s"No such column name as '${name.n}'", tok.pos)
        }
        if (errors.nonEmpty) {
          val (error, pos) = errors.head
          ZIO.fail(Error(error, Location(pos)))
        } else {
          val result = projected.collect {
            case (name, (Some(valueType), _)) => name -> valueType
          }
          val names = nameAndTok.map(_.name)
          val projectedDef = SourceDef(result)
          ZIO.succeed(Checked.Project(names, projectedDef))
        }
      case Ast.Extend(name, expr, pos) =>
        for {
          checkedExpr <- checkExpr(expr, sourceDef)
        } yield {
          val extendedDef = sourceDef.add(name.name, checkedExpr.resultType)
          Checked.Extend(name.name, checkedExpr, extendedDef)
        }
      case Ast.OrderBy(names, order, pos) =>
        val projected = names.map { name => name -> sourceDef.get(name.name) }
        val errors = projected.collect {
          case (name, None) => name.tok.pos -> s"No such column name as '${name.name.n}'"
        }
        if (errors.nonEmpty) {
          ZIO.fail(Error(errors.head._2, Location(errors.head._1)))
        } else {
          val result = Checked.OrderBy(names.map(_.name), order, sourceDef)
          ZIO.succeed(result)
        }
      case Ast.Summarize(aggregations, groupBy, pos) =>
        for {
          checkedAggregations <- ZIO.foreach(aggregations) { case Ast.Aggregation(nameAndTok, aggr) =>
            for {
              checkedAggrCall <- checkAggregationCall(aggr, sourceDef)
            } yield Checked.Aggregation(nameAndTok.name, checkedAggrCall)
          }
          checkedGroupBy <- ZIO.foreach(groupBy) { case NameAndToken(name, tok) =>
            ZIO.fromOption(sourceDef.get(name))
              .map(valueType => name -> valueType)
              .mapError(_ => Error(s"No such column as '${name.n}', used in group by", Location(tok.pos)))
          }
          sourceDef = SourceDef(checkedGroupBy ++ checkedAggregations.map(a => a.name -> a.aggr.resultType))
        } yield Checked.Summarize(checkedAggregations.toSeq, checkedGroupBy.map(_._1), sourceDef)
    }
  }

  def checkTypedExpr[A <: Value: ValueTypeMapper](ast: Ast.Expr, sourceDef: SourceDef): Result[Checked.Expr[A]] = {
    checkExpr(ast, sourceDef).flatMap { expr =>
      val vt = ResolveValueType.valueType[A]
      if (vt != expr.resultType) {
        ZIO.fail(Error(s"Expected expression of '$vt' type, got '${expr.resultType}'", Location(ast.pos)))
      } else {
        ZIO.succeed(expr.asInstanceOf[Checked.Expr[A]])
      }
    }
  }

  def checkExpr(ast: Ast.Expr, sourceDef: SourceDef): Result[Checked.Expr[Value]] = {
    ast match {
      case Ast.Column(name, pos) =>
        for {
          columnType <- sourceDef.get(name) match {
            case Some(columnType) => ZIO.succeed(columnType)
            case None             => ZIO.fail(Error(s"No such column as '${name.n}'", Location(pos)))
          }
        } yield Checked.Column(name, columnType)
      case Ast.NullLit(pos)     => ZIO.succeed(Checked.NullLit)
      case Ast.TrueLit(pos)     => ZIO.succeed(Checked.TrueLit)
      case Ast.FalseLit(pos)    => ZIO.succeed(Checked.FalseLit)
      case Ast.StringLit(value) => ZIO.succeed(Checked.StringLit(value.value))
      case Ast.NumberLit(value) => ZIO.succeed(Checked.NumberLit(value.value))
      //case DateLit(value: Token.DateLit)

      case Ast.UnaryExpr(op, expr, pos) =>
        for {
          checkedExpr <- checkExpr(expr, sourceDef)
          _ <- checkUnaryOp(op, pos, checkedExpr)
        } yield Checked.UnaryExpr(op, checkedExpr)
      case Ast.BinaryExpr(op, exprA, exprB, pos) =>
        for {
          checkedExprA <- checkExpr(exprA, sourceDef)
          checkedExprB <- checkExpr(exprB, sourceDef)
          resultType <- checkBinaryOp(op, pos, checkedExprA, checkedExprB)
        } yield Checked.BinaryExpr(op, checkedExprA, checkedExprB, resultType)
      case fc: Ast.FunctionCall =>
        checkFunctionCall(fc, sourceDef)
    }
  }

  def checkUnaryOp(op: UnaryOp, pos: Int, expr: Checked.Expr[_]): Result[ValueType] = {
    def fail(err: String): Result[Nothing] = ZIO.fail(Error(err, Location(pos)))
    op match {
      case UnaryOp.Plus | UnaryOp.Minus =>
        expr.resultType match {
          case ValueType.Num => ZIO.succeed(ValueType.Num)
          case a => fail(s"Incompatible type $a for unary operator ${op.display}")
        }
      case UnaryOp.Not =>
        expr.resultType match {
          case ValueType.Bool => ZIO.succeed(ValueType.Bool)
          case a => fail(s"Incompatible type $a for unary operator ${op.display}")
        }
    }
  }

  def checkBinaryOp(op: BinaryOp, pos: Int, exprA: Checked.Expr[_], exprB: Checked.Expr[_]): Result[ValueType] = {
    def fail(err: String): Result[Nothing] = ZIO.fail(Error(err, Location(pos)))
    op match {
      case BinaryOp.Plus =>
        (exprA.resultType, exprB.resultType) match {
          case (ValueType.Num, ValueType.Num) => ZIO.succeed(ValueType.Num)
          case (ValueType.Str, ValueType.Str) => ZIO.succeed(ValueType.Str)
          case (a, b) => fail(s"Incompatible types for expression $a ${op.display} $b")
        }
      case BinaryOp.Minus | BinaryOp.Multiply | BinaryOp.Divide =>
        (exprA.resultType, exprB.resultType) match {
          case (ValueType.Num, ValueType.Num) => ZIO.succeed(ValueType.Num)
          case (a, b) => fail(s"Incompatible types for expression $a ${op.display} $b")
        }
      case BinaryOp.Equal | BinaryOp.NotEqual | BinaryOp.Less | BinaryOp.LessEq | BinaryOp.Greater | BinaryOp.GreaterEq | BinaryOp.Assign =>
        if (exprA.resultType == exprB.resultType) {
          ZIO.succeed(ValueType.Bool)
        } else {
          val (a, b) = (exprA.resultType, exprB.resultType)
          fail(s"Incompatible types for expression $a ${op.display} $b")
        }
      case BinaryOp.Contains | BinaryOp.NotContains =>
        (exprA.resultType, exprB.resultType) match {
          case (ValueType.Str, ValueType.Str) => ZIO.succeed(ValueType.Bool)
          case (a, b) => fail(s"Incompatible types for expression $a ${op.display} $b")
        }
      case BinaryOp.And | BinaryOp.Or =>
        (exprA.resultType, exprB.resultType) match {
          case (ValueType.Bool, ValueType.Bool) => ZIO.succeed(ValueType.Bool)
          case (a, b) => fail(s"Incompatible types for expression $a ${op.display} $b")
        }
    }
  }

  def checkFunctionCall(ast: Ast.FunctionCall, sourceDef: SourceDef): Result[Checked.FunctionCall[Value]] = {
    for {
      functionDef <- ZIO.accessM[Env] { env =>
        ZIO.fromOption(env.functionDef(ast.functionName))
          .mapError(_ => Error(s"No such function as '${ast.functionName.n}' found", Location(ast.pos)))
      }
      // TODO: check parameters
      args <- ZIO.foreach(ast.args) { expr => checkExpr(expr, sourceDef) }
    } yield Checked.FunctionCall(functionDef, args.toSeq)
  }

  def checkAggregationCall(ast: Ast.FunctionCall, sourceDef: SourceDef): Result[Checked.AggregationCall[Value]] = {
    for {
      aggregationDef <- ZIO.accessM[Env] { env =>
        ZIO.fromOption(env.aggregationDef(ast.functionName))
          .mapError(_ => Error(s"No such aggregation function as '${ast.functionName.n}' found", Location(ast.pos)))
      }
      // TODO: check parameters
      args <- ZIO.foreach(ast.args) { expr => checkExpr(expr, sourceDef) }
    } yield Checked.AggregationCall(aggregationDef, args.toSeq)
  }

}


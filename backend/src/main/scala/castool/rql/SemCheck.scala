package castool.rql
import scala.collection.SeqMap
import ResolveValueType._
import scala.collection.mutable.Buffer

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

  //case class Expr[A <: Value](fn: InputRow => A, resultType: ValueType)
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
  sealed trait Result[+A] extends Product {
    def isSuccess: Boolean
    def isFailure: Boolean
    def map[B](f: A => B): Result[B] = this match {
      case Success(x) => Success(f(x))
      case Failure(_) => this.asInstanceOf[Result[B]]
    }
    def flatMap[B](f: A => Result[B]): Result[B] = this match {
      case Success(x) => f(x)
      case Failure(_) => this.asInstanceOf[Result[B]]
    }
  }
  final case class Success[A](result: A) extends Result[A] {
    def isSuccess: Boolean = true
    def isFailure: Boolean = false
  }
  final case class Failure(messages: Seq[String]) extends Result[Nothing] {
    def isSuccess: Boolean = false
    def isFailure: Boolean = true
  }

  object Result {
    def success[A](a: A): Result[A] = Success(a)
    def failure[A](msg: String): Result[A] = Failure(msg)
    def failure[A](msgs: Seq[String]): Result[A] = Failure(msgs)
    def traverse[A, B](xs: Iterable[A])(f: A => Result[B]): Result[Iterable[B]] = {
      xs.foldLeft(Result.success(Buffer.empty[B])) {
        case (results, x) =>
          for {
            rs <- results
          } yield rs

          for {
            rs <- results
            fx <- f(x)
          } yield rs :+ fx
      }.map(ys => ys.toIterable)
    }
  }
  object Failure {
    def apply(msg: String): Failure = Failure(Seq(msg))
  }

  trait Env {
    def tableDef(name: Name): Result[SourceDef]
    def functionDef(name: Name): Result[FunctionDef[Value]]
    def aggregationDef(name: Name): Result[AggregationDef[Value]]
  }

  def check(ast: Ast.Source, env: Env): Result[Checked.Source] = {
    ast match {
      case Ast.Table(name, tok) =>
        for {
          tableDef <- env.tableDef(name)
        } yield Checked.Table(name, tableDef)
      case Ast.Cont(source, op) =>
        for {
          checkedSource <- check(source, env)
          checkedOp <- checkOp(op, checkedSource, env)
        } yield Checked.Cont(checkedSource, checkedOp)
    }
  }

  def checkOp(ast: Ast.TopLevelOp, source: Checked.Source, env: Env): Result[Checked.TopLevelOp] = {
    ast match {
      case Ast.Where(expr, pos) =>
        for {
          checkedExpr <- checkTypedExpr[Bool](expr, source.sourceDef, env)
        } yield Checked.Where(checkedExpr, source.sourceDef)
      case Ast.Project(nameAndTok, pos) =>
        val names = nameAndTok.map(_.name)
        source.sourceDef.project(names) match {
          case Right(projectedDef) =>
            Success(Checked.Project(names, projectedDef))
          case Left(errors) =>
            Failure(errors)
        }
      case Ast.Extend(name, expr, pos) =>
        for {
          checkedExpr <- checkExpr(expr, source.sourceDef, env)
          extendedDef <- source.sourceDef.extend(name.name, checkedExpr.resultType) match {
            case Right(extendedDef) => Success(extendedDef)
            case Left(errors)       => Failure(errors)
          }
        } yield Checked.Extend(name.name, checkedExpr, extendedDef)
      case Ast.OrderBy(names, order, pos) =>
        val projected = names.map { name => name -> source.sourceDef.get(name.name) }
        val errors = projected.collect {
          case (name, None) => s"No such column name as '${name.name.n}'"
        }
        if (errors.nonEmpty) {
          Failure(errors)
        } else {
          val result = Checked.OrderBy(names.map(_.name), order, source.sourceDef)
          Success(result)
        }
      case Ast.Summarize(aggregations, groupBy, pos) =>
        for {
          checkedAggregations <- Result.traverse(aggregations) { case Ast.Aggregation(nameAndTok, aggr) =>
            for {
              checkedAggrCall <- checkAggregationCall(aggr, source.sourceDef, env)
            } yield Checked.Aggregation(nameAndTok.name, checkedAggrCall)
          }
          checkedGroupBy = groupBy.map(_.name) // TODO: implement check
          sourceDef = SourceDef(checkedAggregations.map(a => a.name -> a.aggr.resultType))
        } yield Checked.Summarize(checkedAggregations.toSeq, checkedGroupBy, sourceDef)

    }
  }

  def checkTypedExpr[A <: Value: ValueTypeMapper](ast: Ast.Expr, sourceDef: SourceDef, env: Env): Result[Checked.Expr[A]] = {
    checkExpr(ast, sourceDef, env).flatMap { expr =>
      val vt = ResolveValueType.valueType[A]
      if (vt != expr.resultType) {
        Failure(s"Expected expression of '$vt' type, got '${expr.resultType}'")
      } else {
        Success(expr.asInstanceOf[Checked.Expr[A]])
      }
    }
  }

  def checkExpr(ast: Ast.Expr, sourceDef: SourceDef, env: Env): Result[Checked.Expr[Value]] = {
    ast match {
      case Ast.Column(name, pos) =>
        for {
          columnType <- sourceDef.get(name) match {
            case Some(columnType) => Result.success(columnType)
            case None => Result.failure(s"No such column as '${name.n}'")
          }
        } yield Checked.Column(name, columnType)
      case Ast.NullLit(pos) => Result.success(Checked.NullLit)
      case Ast.TrueLit(pos) => Result.success(Checked.TrueLit)
      case Ast.FalseLit(pos) => Result.success(Checked.FalseLit)
      case Ast.StringLit(value) => Result.success(Checked.StringLit(value.value))
      case Ast.NumberLit(value) => Result.success(Checked.NumberLit(value.value))
      //case DateLit(value: Token.DateLit)

      case Ast.UnaryExpr(op, expr, pos) =>
        for {
          checkedExpr <- checkExpr(expr, sourceDef, env)
          _ <- checkUnaryOp(op, checkedExpr)
        } yield Checked.UnaryExpr(op, checkedExpr)
      case Ast.BinaryExpr(op, exprA, exprB, pos) =>
        for {
          checkedExprA <- checkExpr(exprA, sourceDef, env)
          checkedExprB <- checkExpr(exprB, sourceDef, env)
          resultType <- checkBinaryOp(op, checkedExprA, checkedExprB)
        } yield Checked.BinaryExpr(op, checkedExprA, checkedExprB, resultType)
      case fc: Ast.FunctionCall => checkFunctionCall(fc, sourceDef, env)
    }
  }

  def checkUnaryOp(op: UnaryOp, expr: Checked.Expr[_]): Result[ValueType] = {
    op match {
      case UnaryOp.Plus | UnaryOp.Minus =>
        expr.resultType match {
          case ValueType.Num => Result.success(ValueType.Num)
          case a => Result.failure(s"Incompatible type $a for unary operator ${op.display}")
        }
      case UnaryOp.Not =>
        expr.resultType match {
          case ValueType.Bool => Result.success(ValueType.Bool)
          case a => Result.failure(s"Incompatible type $a for unary operator ${op.display}")
        }
    }
  }

  def checkBinaryOp(op: BinaryOp, exprA: Checked.Expr[_], exprB: Checked.Expr[_]): Result[ValueType] = {
    op match {
      case BinaryOp.Plus =>
        (exprA.resultType, exprB.resultType) match {
          case (ValueType.Num, ValueType.Num) => Result.success(ValueType.Num)
          case (ValueType.Str, ValueType.Str) => Result.success(ValueType.Str)
          case (a, b) => Result.failure(s"Incompatible types for expression $a ${op.display} $b")
        }
      case BinaryOp.Minus | BinaryOp.Multiply | BinaryOp.Divide =>
        (exprA.resultType, exprB.resultType) match {
          case (ValueType.Num, ValueType.Num) => Result.success(ValueType.Num)
          case (a, b) => Result.failure(s"Incompatible types for expression $a ${op.display} $b")
        }
      case BinaryOp.Equal | BinaryOp.NotEqual | BinaryOp.Less | BinaryOp.LessEq | BinaryOp.Greater | BinaryOp.GreaterEq | BinaryOp.Assign =>
        if (exprA.resultType == exprB.resultType) {
          Result.success(ValueType.Bool)
        } else {
          val (a, b) = (exprA.resultType, exprB.resultType)
          Result.failure(s"Incompatible types for expression $a ${op.display} $b")
        }
      case BinaryOp.Contains | BinaryOp.NotContains =>
        (exprA.resultType, exprB.resultType) match {
          case (ValueType.Str, ValueType.Str) => Result.success(ValueType.Bool)
          case (a, b) => Result.failure(s"Incompatible types for expression $a ${op.display} $b")
        }
      case BinaryOp.And | BinaryOp.Or =>
        (exprA.resultType, exprB.resultType) match {
          case (ValueType.Bool, ValueType.Bool) => Result.success(ValueType.Bool)
          case (a, b) => Result.failure(s"Incompatible types for expression $a ${op.display} $b")
        }
    }
  }

  def checkFunctionCall(ast: Ast.FunctionCall, sourceDef: SourceDef, env: Env): Result[Checked.FunctionCall[Value]] = {
    for {
      functionDef <- env.functionDef(ast.functionName)
      args <- Result.traverse(ast.args) { expr => checkExpr(expr, sourceDef, env) }
    } yield Checked.FunctionCall(functionDef, args.toSeq)
  }

  def checkAggregationCall(ast: Ast.FunctionCall, sourceDef: SourceDef, env: Env): Result[Checked.AggregationCall[Value]] = {
    for {
      aggregationDef <- env.aggregationDef(ast.functionName)
      args <- Result.traverse(ast.args) { expr => checkExpr(expr, sourceDef, env) }
    } yield Checked.AggregationCall(aggregationDef, args.toSeq)
  }

}


package castool.rql

import scala.collection.SeqMap
import zio._

import ResolveValueType._

object SemCheck {
  case class Error(msg: String, loc: Location)
  type Result[+A] = ZIO[Env, Error, A]

  trait Env {
    def tableDef(name: Name): Option[SourceDef]
    def functionDef(name: Name, argumentTypes: Seq[ValueType]): Option[FunctionDef[Value]]
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
      case Ast.Project(names, pos) =>
        for {
          projected <- ZIO.foreach(names) {
            case column: Ast.Column =>
              for {
                checkedExpr <- checkExpr(column, sourceDef)
              } yield Checked.Assignment(column.name, checkedExpr)
            case Ast.AssignmentExpr(nameAndTok, expr, pos) =>
              for {
                checkedExpr <- checkExpr(expr, sourceDef)
              } yield Checked.Assignment(nameAndTok.name, checkedExpr)
          }
          projectedDef = SourceDef(projected.map {
            case Checked.Assignment(name, expr) => name -> expr.resultType
          })
        } yield Checked.Project(projected, projectedDef)
      case Ast.Extend(assignExpr, pos) =>
        for {
          checkedExpr <- checkAssignment(assignExpr, sourceDef)
        } yield {
          val extendedDef = sourceDef.add(checkedExpr.name, checkedExpr.resultType)
          Checked.Extend(checkedExpr, extendedDef)
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
          checkedGroupBy <- ZIO.foreach(groupBy) {
            case column: Ast.Column =>
              for {
                checkedExpr <- checkExpr(column, sourceDef)
              } yield Checked.Assignment(column.name, checkedExpr)
            case Ast.AssignmentExpr(nameAndTok, expr, pos) =>
              for {
                checkedExpr <- checkExpr(expr, sourceDef)
              } yield Checked.Assignment(nameAndTok.name, checkedExpr)
          }
          groupByNames = checkedGroupBy.map {
            case Checked.Assignment(name, expr) => name -> expr.resultType
          }
          aggregationNames = checkedAggregations.map {
            case Checked.Aggregation(name, aggr) => name -> aggr.resultType
          }
          sourceDef = SourceDef(groupByNames ++ aggregationNames)
        } yield Checked.Summarize(checkedAggregations.toSeq, checkedGroupBy, sourceDef)
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

  def checkAssignment(ast: Ast.AssignmentExpr, sourceDef: SourceDef): Result[Checked.Assignment] = {
    checkExpr(ast.expr, sourceDef).map { expr =>
      Checked.Assignment(ast.name.name, expr)
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
      case Ast.DateLit(value)   => ZIO.succeed(Checked.DateLit(value.value))

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
      case a: Ast.AssignmentExpr => throw new MatchError(a)
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
          case (ValueType.Blob, ValueType.Blob) => ZIO.succeed(ValueType.Blob)
          case (a, b) => fail(s"Incompatible types for expression $a ${op.display} $b")
        }
      case BinaryOp.Minus | BinaryOp.Multiply | BinaryOp.Divide =>
        (exprA.resultType, exprB.resultType) match {
          case (ValueType.Num, ValueType.Num) => ZIO.succeed(ValueType.Num)
          case (a, b) => fail(s"Incompatible types for expression $a ${op.display} $b")
        }
      case BinaryOp.Equal | BinaryOp.NotEqual | BinaryOp.Less | BinaryOp.LessEq | BinaryOp.Greater | BinaryOp.GreaterEq =>
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

  def assignCompatible(to: ValueType, from: ValueType): Boolean = {
    (to == from) || from == ValueType.Null
  }

  def checkFunctionCall(ast: Ast.FunctionCall, sourceDef: SourceDef): Result[Checked.FunctionCall[Value]] = {
    for {
      args <- ZIO.foreach(ast.args) { expr => checkExpr(expr, sourceDef) }
      functionDef <- ZIO.accessM[Env] { env =>
        ZIO.fromOption(env.functionDef(ast.functionName, args.map(_.resultType)))
          .mapError(_ => Error(s"No such function as '${ast.functionName.n}' found", Location(ast.pos)))
      }
      _ <- checkArgs(ast, functionDef, args)
    } yield Checked.FunctionCall(functionDef, args)
  }

  def checkArgs(ast: Ast.FunctionCall, functionDef: FunctionDef[_], args: Seq[Checked.Expr[_]]): Result[Unit] = {
    if (functionDef.parameters.size != args.size) {
      ZIO.fail(Error(s"Function '${ast.functionName.n}' expecting ${functionDef.parameters.size} arguments, got ${args.size}", Location(ast.pos)))
    } else {
      ast.args.map(_.pos).zip(args).zip(functionDef.parameters).find {
        case ((pos, checked), (name, valueType)) =>
          !assignCompatible(valueType, checked.resultType)
      } match {
        case Some(((pos, checked), (name, valueType))) =>
          val err = s"Expected $valueType for parameter '$name', got ${checked.resultType}"
          ZIO.fail(Error(err, Location(pos)))
        case None =>
          ZIO.unit
      }
    }
  }

  def checkAggregationCall(ast: Ast.FunctionCall, sourceDef: SourceDef): Result[Checked.AggregationCall[Value]] = {
    for {
      aggregationDef <- ZIO.accessM[Env] { env =>
        ZIO.fromOption(env.aggregationDef(ast.functionName))
          .mapError(_ => Error(s"No such aggregation function as '${ast.functionName.n}' found", Location(ast.pos)))
      }
      args <- ZIO.foreach(ast.args) { expr => checkExpr(expr, sourceDef) }
      _ <- checkAggregationArgs(ast, aggregationDef, args)
    } yield Checked.AggregationCall(aggregationDef, args)
  }

  def checkAggregationArgs(ast: Ast.FunctionCall, aggregationDef: AggregationDef[_], args: Seq[Checked.Expr[_]]): Result[Unit] = {
    // Drop the first parameter, as it is the accumulator, passed in implicitly during execution.
    val parameters = aggregationDef.parameters.drop(1)
    if (parameters.size != args.size) {
      ZIO.fail(Error(s"Aggregation function '${ast.functionName.n}' expecting ${parameters.size} arguments, got ${args.size}", Location(ast.pos)))
    } else {
      ast.args.map(_.pos).zip(args).zip(parameters).find {
        case ((pos, checked), (name, paramType)) =>
          !assignCompatible(paramType, checked.resultType)
      } match {
        case Some(((pos, checked), (name, paramType))) =>
          val err = s"Expected $paramType for parameter '$name', got ${checked.resultType}"
          ZIO.fail(Error(err, Location(pos)))
        case None =>
          ZIO.unit
      }
    }
  }

}


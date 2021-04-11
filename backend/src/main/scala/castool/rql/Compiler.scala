package castool.rql

import zio._
import zio.stream._

import scala.collection.SeqMap

sealed trait Compiled extends Serializable with Product

object Compiled {
  type EnvService = zio.Has[Env]
  type Stream = ZStream[EnvService, Throwable, InputRow]

  trait Env {
    def table(name: Name): Option[Stream]
    def tableDef(name: Name): Option[SourceDef]
    def function(name: Name): Option[FunctionDef[_]]
    def aggregation(name: Name): Option[FunctionDef[_]]
  }

  def live(env: Env): ZLayer[Any, Nothing, zio.Has[Env]] = ZLayer.fromEffect {
    ZIO.succeed(env)
  }

  sealed trait Source extends Compiled { def stream: Stream }
  final case class Table(name: Name) extends Source {
    def stream: Stream = ZStream.accessStream[EnvService] { service =>
      val env = service.get
      env.table(name) match {
        case Some(stream) => stream
        case None =>
          val err = "Runtime: No such table as'" + name.n + "'"
          ZStream.fail(
            new NoSuchElementException()
          )
      }
    }
  }
  final case class Cont(source: Source, op: TopLevelOp) extends Source {
    def stream: Stream = op.transform(source.stream)
  }

  trait Expr[A <: Value] { def eval(input: InputRow): A }

  sealed trait TopLevelOp extends Compiled { def transform(source: Stream): Stream }
  final case class Where(expr: Expr[Bool]) extends TopLevelOp {
    def transform(source: Stream): Stream = source.filter((expr.eval _).andThen(_.v))
  }
  final case class Project(names: Seq[Name]) extends TopLevelOp {
    def transform(source: Stream): Stream = source.map { input =>
      val values = names.map(name => name -> input.values(name))
      InputRow(values)
    }
  }
  final case class Extend(name: Name, expr: Expr[Value]) extends TopLevelOp {
    def transform(source: Stream): Stream = source.map { input =>
      val value = expr.eval(input)
      val newValue = (name, value)
      InputRow(input.values + newValue)
    }
  }
  private case class OrderValues(xs: Seq[Value])
  private implicit val ordering: Ordering[OrderValues] = new Ordering[OrderValues] {
    override def compare(as: OrderValues, bs: OrderValues): Int = {
      as.xs.zip(bs.xs).foldLeft(0) { case (acc, (a, b)) =>
        if (acc == 0) {
          (a, b) match {
            case (Null, Null) => 0
            case (Null, _) => -1
            case (_, Null) => 1
            case (Bool(true), Bool(true)) => 0
            case (Bool(false), Bool(true)) => -1
            case (Bool(true), Bool(false)) => 1
            case (Bool(false), Bool(false)) => 0
            case (Num(x), Num(y)) => if (x == y) 0 else if (x < y) -1 else 1
            case (Str(x), Str(y)) => x.compare(y)
            case (Obj(x), Obj(y)) => -1 // TODO: figure out how to compare or treat as an error in sem check
            case x => throw new MatchError(x)
          }
        } else {
          acc
        }
      }
    }
  }
  final case class OrderBy(names: Seq[Name], order: Order) extends TopLevelOp {
    def transform(source: Stream): Stream = {
      def orderBy: InputRow => OrderValues = { input =>
        val values = names.map(name => input.values(name))
        OrderValues(values)
      }
      val folded = source.fold(Vector.empty[InputRow]) { case (acc, input) => acc :+ input }
      val it = folded.map(_.sortBy(orderBy)(if (order == Order.Asc) ordering else ordering.reverse))
      ZStream.fromIterableM(it)
    }
  }

  case class Aggregation(name: Name, initialValue: Value, aggr: (Value, InputRow) => Value, finalPhase: (Value, Num) => Value)
  final case class Summarize(aggregations: Seq[Aggregation], groupBy: Seq[Name]) extends TopLevelOp {
    def transform(source: Stream): Stream = {
      val initialValues = aggregations.map(_.initialValue)
      if (groupBy.isEmpty) {
        val result = source.fold(initialValues) { case (values, input) =>
          values.zip(aggregations).map { case (acc, aggregation) =>
            aggregation.aggr(acc, input)
          }
        }.map(values => InputRow(aggregations.map(_.name).zip(values)))
        ZStream.fromEffect(result)
      } else {
        val keyFunc = (input: InputRow) => groupBy.map(input.values)
        source.groupByKey(keyFunc).apply { case (key, stream) =>
          val result = stream
            .fold((initialValues, 0)) { case ((values, n), input) =>
              val results = values
                .zip(aggregations)
                .map { case (acc, aggregation) => aggregation.aggr(acc, input) }
              (results, n + 1)
            }
            .map { case (values, n) =>
              val results = aggregations
                .zip(values)
                .map { case (aggregation, value) =>
                  val finalized = aggregation.finalPhase(value, Num(n))
                  (aggregation.name, finalized)
                }
              InputRow(results)
            }
          ZStream.fromEffect(result)
        }
      }
    }
  }

}

case class CompiledQuery(q: Compiled)

object Compiler {
  case class Error(error: String, loc: SourceLocation)

  type CompiledIO[A] = ZIO[Compiled.Env, Error, A]

  def compileQuery(q: String): CompiledIO[Compiled.Source] = {
    for {
      tokens <- Lexer.lex(q) match {
          case Lexer.Success(tokens) =>
            ZIO.succeed(tokens)
          case Lexer.Failure(msg, pos) =>
            ZIO.fail(Error(msg, Location(pos).atSourceText(q)))
        }
      ast <- Parser.parse(tokens) match {
          case Parser.Success(ast) =>
            ZIO.succeed(ast)
          case Parser.Failure(msg, pos) =>
            ZIO.fail(Error(msg, Location(pos).atSourceText(q)))
        }
      semCheckEnv <- ZIO.accessM[Compiled.Env] { env =>
        val scEnv = new SemCheck.Env {
          def tableDef(name: Name): SemCheck.Result[SourceDef] = env.tableDef(name) match {
            case Some(result) => SemCheck.Result.success(result)
            case None => SemCheck.Result.failure(s"No table def found for '${name.n}'")
          }
          def functionDef(name: Name): SemCheck.Result[FunctionDef[Value]] = env.function(name) match {
            case Some(result) => SemCheck.Result.success(result.asInstanceOf[FunctionDef[Value]])
            case None => SemCheck.Result.failure(s"No function def found for '${name.n}'")
          }
          def aggregationDef(name: Name): SemCheck.Result[AggregationDef[Value]] = env.aggregation(name) match {
            case Some(result) => SemCheck.Result.success(result.asInstanceOf[AggregationDef[Value]])
            case None => SemCheck.Result.failure(s"No aggregation def found for '${name.n}'")
          }
        }
        ZIO.succeed(scEnv)
      }
      checked <- SemCheck.check(ast, semCheckEnv) match {
        case SemCheck.Success(checked) => ZIO.succeed(checked)
        case SemCheck.Failure(msgs) =>
            ZIO.fail(Error(msgs.mkString("; "), Location(0).atSourceText(q)))
      }
      result <- compile(checked)
    } yield result
  }

  def compile(checked: Checked.Source): CompiledIO[Compiled.Source] = {
    checked match {
      case Checked.Table(name, _) =>
        ZIO.succeed(Compiled.Table(name))
      case Checked.Cont(source, op) =>
        for {
          compiledSource <- compile(source)
          compiledOp <- compileTopLevelOp(op)
        } yield Compiled.Cont(compiledSource, compiledOp)
    }
  }

  def compileTopLevelOp(checked: Checked.TopLevelOp): CompiledIO[Compiled.TopLevelOp] = {
    checked match {
      case Checked.Where(expr, _) =>
        for {
          compiledExpr <- compileExpr(expr)
        } yield Compiled.Where(compiledExpr)
      case Checked.Project(names, _) =>
        ZIO.succeed(Compiled.Project(names))
      case Checked.Extend(name, expr, _) =>
        for {
          compiledExpr <- compileExpr[Value](expr)
        } yield Compiled.Extend(name, compiledExpr)
      case Checked.OrderBy(names, order, _) =>
        ZIO.succeed(Compiled.OrderBy(names, order))
      case Checked.Summarize(aggregations, groupBy, _) =>
        for {
          compiledAggrs <- ZIO.foreach(aggregations)(compileAggregationCall)
        } yield Compiled.Summarize(compiledAggrs, groupBy)
    }
  }

  def compileAggregationCall(checked: Checked.Aggregation): CompiledIO[Compiled.Aggregation] = {
    val Checked.Aggregation(name, Checked.AggregationCall(aggregationDef, args)) = checked
    for {
      compiledArgs <- ZIO.foreach(args)(compileExpr)
    } yield {
      val aggr = (acc: Value, input: InputRow) => {
        val arguments = compiledArgs.map(_.eval(input))
        aggregationDef.evaluate(acc +: arguments)
      }
      Compiled.Aggregation(name, aggregationDef.initialValue, aggr, aggregationDef.finalPhase)
    }
  }

  def compileExpr[A <: Value](expr: Checked.Expr[A]): CompiledIO[Compiled.Expr[A]] = {
    expr match {
      case Checked.Column(name, _) =>
        ZIO.succeed((input: InputRow) => input.values(name).asInstanceOf[A])
      case Checked.NullLit =>
        ZIO.succeed((input: InputRow) => Null.asInstanceOf[A])
      case Checked.TrueLit | Checked.FalseLit =>
        if (expr.resultType != ValueType.Bool) {
          ZIO.fail(Error(s"Boolean expression expected, got '${expr.resultType}'", SourceLocation(0, 0)))
        } else {
          ZIO.succeed((input: InputRow) => Bool(expr == Checked.TrueLit).asInstanceOf[A])
        }
      case Checked.NumberLit(value) =>
        if (expr.resultType != ValueType.Num) {
          ZIO.fail(Error(s"Numerical expression expected, got '${expr.resultType}'", SourceLocation(0, 0)))
        } else {
          ZIO.succeed((input: InputRow) => Num(value).asInstanceOf[A])
        }
      case Checked.StringLit(value) =>
        if (expr.resultType != ValueType.Str) {
          ZIO.fail(Error(s"String expression expected, got '${expr.resultType}'", SourceLocation(0, 0)))
        } else {
          ZIO.succeed((input: InputRow) => Str(value).asInstanceOf[A])
        }
      case Checked.UnaryExpr(op, expr) =>
        for {
          compiledExpr <- compileExpr(expr)
        } yield op match {
          case UnaryOp.Plus =>
            compiledExpr
          case UnaryOp.Minus =>
            new Compiled.Expr[A] {
              def eval(input: InputRow): A = compiledExpr.eval(input) match {
                case Num(value) => Num(-value).asInstanceOf[A]
                case x => throw new MatchError(x)
              }
            }
          case UnaryOp.Not =>
            new Compiled.Expr[A] {
              def eval(input: InputRow): A = compiledExpr.eval(input) match {
                case Bool(value) => Bool(!value).asInstanceOf[A]
                case x => throw new MatchError(x)
              }
            }
        }
      case Checked.BinaryExpr(op, exprA, exprB, _) =>
        for {
          compiledA <- compileExpr(exprA)
          compiledB <- compileExpr(exprB)
        } yield op match {
          case BinaryOp.Plus => (input: InputRow) => {
              val va = compiledA.eval(input)
              val vb = compiledB.eval(input)
              (va, vb) match {
                case (Num(a), Num(b)) => Num(a + b).asInstanceOf[A]
                case (Str(a), Str(b)) => Str(a + b).asInstanceOf[A]
                case (a, b) => throw new MatchError((a, op, b))
              }
            }
          case BinaryOp.Minus => (input: InputRow) => {
              val va = compiledA.eval(input)
              val vb = compiledB.eval(input)
              (va, vb) match {
                case (Num(a), Num(b)) => Num(a - b).asInstanceOf[A]
                case (a, b) => throw new MatchError((a, op, b))
              }
            }

          case BinaryOp.Multiply => (input: InputRow) => {
              val va = compiledA.eval(input)
              val vb = compiledB.eval(input)
              (va, vb) match {
                case (Num(a), Num(b)) => Num(a * b).asInstanceOf[A]
                case (a, b) => throw new MatchError((a, op, b))
              }
            }

          case BinaryOp.Divide => (input: InputRow) => {
              val va = compiledA.eval(input)
              val vb = compiledB.eval(input)
              (va, vb) match {
                case (Num(a), Num(b)) => Num(a / b).asInstanceOf[A]
                case (a, b) => throw new MatchError((a, op, b))
              }
            }

          case BinaryOp.Equal => (input: InputRow) => {
              val va = compiledA.eval(input)
              val vb = compiledB.eval(input)
              Bool(va == vb).asInstanceOf[A]
            }

          case BinaryOp.NotEqual => (input: InputRow) => {
              val va = compiledA.eval(input)
              val vb = compiledB.eval(input)
              Bool(va != vb).asInstanceOf[A]
            }

          case BinaryOp.Less => (input: InputRow) => {
              val va = compiledA.eval(input)
              val vb = compiledB.eval(input)
              (va, vb) match {
                case (Num(a), Num(b)) => Bool(a < b).asInstanceOf[A]
                case (Str(a), Str(b)) => Bool(a < b).asInstanceOf[A]
                case (a, b) => throw new MatchError((a, op, b))
              }
            }

          case BinaryOp.LessEq => (input: InputRow) => {
              val va = compiledA.eval(input)
              val vb = compiledB.eval(input)
              (va, vb) match {
                case (Num(a), Num(b)) => Bool(a <= b).asInstanceOf[A]
                case (Str(a), Str(b)) => Bool(a <= b).asInstanceOf[A]
                case (a, b) => throw new MatchError((a, op, b))
              }
            }

          case BinaryOp.Greater => (input: InputRow) => {
              val va = compiledA.eval(input)
              val vb = compiledB.eval(input)
              (va, vb) match {
                case (Num(a), Num(b)) => Bool(a > b).asInstanceOf[A]
                case (Str(a), Str(b)) => Bool(a > b).asInstanceOf[A]
                case (a, b) => throw new MatchError((a, op, b))
              }
            }

          case BinaryOp.GreaterEq => (input: InputRow) => {
              val va = compiledA.eval(input)
              val vb = compiledB.eval(input)
              (va, vb) match {
                case (Num(a), Num(b)) => Bool(a >= b).asInstanceOf[A]
                case (Str(a), Str(b)) => Bool(a >= b).asInstanceOf[A]
                case (a, b) => throw new MatchError((a, op, b))
              }
            }

          case BinaryOp.Contains => (input: InputRow) => {
              val va = compiledA.eval(input)
              val vb = compiledB.eval(input)
              (va, vb) match {
                case (Str(a), Str(b)) => Bool(a.contains(b)).asInstanceOf[A]
              }
            }
          case BinaryOp.NotContains => (input: InputRow) => {
              val va = compiledA.eval(input)
              val vb = compiledB.eval(input)
              (va, vb) match {
                case (Str(a), Str(b)) => Bool(!a.contains(b)).asInstanceOf[A]
              }
            }

          case BinaryOp.And => (input: InputRow) => {
              val va = compiledA.eval(input)
              val vb = compiledB.eval(input)
              (va, vb) match {
                case (Bool(a), Bool(b)) => Bool(a && b).asInstanceOf[A]
              }
            }

          case BinaryOp.Or => (input: InputRow) => {
              val va = compiledA.eval(input)
              val vb = compiledB.eval(input)
              (va, vb) match {
                case (Bool(a), Bool(b)) => Bool(a || b).asInstanceOf[A]
              }
            }

          case BinaryOp.Assign => throw new MatchError(op)
        }

      case Checked.FunctionCall(functionDef, args) =>
        for {
          compiledArgs <- ZIO.foreach(args)(compileExpr)
        } yield (input: InputRow) => {
          val arguments = compiledArgs.map(_.eval(input))
          functionDef.evaluate(arguments)
        }
    }
  }

}


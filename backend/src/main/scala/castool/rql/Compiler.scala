package castool.rql

import zio._

import scala.collection.SeqMap

sealed trait Compiled extends Serializable with Product {
  def sourceDef: SourceDef
}

object Compiled {
  sealed trait Source extends Compiled
  final case class Table(name: Name, sourceDef: SourceDef) extends Source
  final case class Cont(source: Source, op: TopLevelOp) extends Source { def sourceDef: SourceDef = op.sourceDef }

  trait Expr[A <: Value] { def eval(input: InputRow): A }

  sealed trait TopLevelOp extends Compiled
  final case class Where(expr: Expr[Bool], sourceDef: SourceDef) extends TopLevelOp
  final case class Project(names: Seq[Name], sourceDef: SourceDef) extends TopLevelOp
  final case class Assignment[A <: Value](name: Name, expr: Expr[A])
  final case class Extend(assign: Assignment[Value], sourceDef: SourceDef) extends TopLevelOp
  final case class OrderBy(names: Seq[Name], order: Order, sourceDef: SourceDef) extends TopLevelOp
  final case class Aggregation(name: Name, initialValue: Value, aggr: (Value, InputRow) => Value, finalPhase: (Value, Num) => Value)
  final case class Summarize(aggregations: Seq[Aggregation], groupBy: Seq[Name], sourceDef: SourceDef) extends TopLevelOp
}

object Compiler {
  case class Error(message: String, loc: SourceLocation)
  type Errors = Seq[Error]

  trait Env extends SemCheck.Env {
  }

  type CompiledIO[A] = ZIO[Env, Errors, A]

  def compileQuery(q: String): CompiledIO[Compiled.Source] = {
    for {
      tokens <- Lexer.lex(q) match {
        case Lexer.Success(tokens) =>
          ZIO.succeed(tokens)
        case Lexer.Failure(msg, location) =>
          ZIO.fail(Error(msg, location.atSourceText(q)) :: Nil)
      }
      ast <- Parser.parse(tokens) match {
        case Parser.Success(ast) =>
          ZIO.succeed(ast)
        case Parser.Failure(errors) =>
          val errs = errors.map(err => Error(err.message, err.loc.atSourceText(q)))
          ZIO.fail(errs)
      }
      checked <- SemCheck.check(ast).mapError {
        case SemCheck.Error(msg, location) =>
          Error(msg, location.atSourceText(q)) :: Nil
      }
      result <- compile(checked)
    } yield result
  }

  def compile(checked: Checked.Source): CompiledIO[Compiled.Source] = {
    checked match {
      case Checked.Table(name, sourceDef) =>
        ZIO.succeed(Compiled.Table(name, sourceDef))
      case Checked.Cont(source, op) =>
        for {
          compiledSource <- compile(source)
          compiledOp <- compileTopLevelOp(op)
        } yield Compiled.Cont(compiledSource, compiledOp)
    }
  }

  def compileTopLevelOp(checked: Checked.TopLevelOp): CompiledIO[Compiled.TopLevelOp] = {
    checked match {
      case Checked.Where(expr, sourceDef) =>
        for {
          compiledExpr <- compileExpr(expr)
        } yield Compiled.Where(compiledExpr, sourceDef)
      case Checked.Project(names, sourceDef) =>
        ZIO.succeed(Compiled.Project(names, sourceDef))
      case Checked.Extend(assign, sourceDef) =>
        for {
          compiled <- compileAssignment[Value](assign)
        } yield Compiled.Extend(compiled, sourceDef)
      case Checked.OrderBy(names, order, sourceDef) =>
        ZIO.succeed(Compiled.OrderBy(names, order, sourceDef))
      case Checked.Summarize(aggregations, groupBy, sourceDef) =>
        for {
          compiledAggrs <- ZIO.foreach(aggregations)(compileAggregationCall)
        } yield Compiled.Summarize(compiledAggrs, groupBy, sourceDef)
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
        ZIO.succeed((input: InputRow) => Bool(expr == Checked.TrueLit).asInstanceOf[A])
      case Checked.NumberLit(value) =>
        ZIO.succeed((input: InputRow) => Num(value).asInstanceOf[A])
      case Checked.DateLit(value) =>
        ZIO.succeed((input: InputRow) => value.asInstanceOf[A])
      case Checked.StringLit(value) =>
        ZIO.succeed((input: InputRow) => Str(value).asInstanceOf[A])
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
                case (a: Blob, b: Blob) => Blob.concat(a, b).asInstanceOf[A]
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
              val result = (va, vb) match {
                case (Blob(a), Blob(b)) => a.sameElements(b)
                case (a, b) => a == b
              }
              Bool(result).asInstanceOf[A]
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
                case (Date(a), Date(b)) => Bool(a.isBefore(b)).asInstanceOf[A]
                case (Str(a), Str(b)) => Bool(a < b).asInstanceOf[A]
                case (a, b) => throw new MatchError((a, op, b))
              }
            }

          case BinaryOp.LessEq => (input: InputRow) => {
              val va = compiledA.eval(input)
              val vb = compiledB.eval(input)
              (va, vb) match {
                case (Num(a), Num(b)) => Bool(a <= b).asInstanceOf[A]
                case (Date(a), Date(b)) => Bool(!b.isBefore(a)).asInstanceOf[A]
                case (Str(a), Str(b)) => Bool(a <= b).asInstanceOf[A]
                case (a, b) => throw new MatchError((a, op, b))
              }
            }

          case BinaryOp.Greater => (input: InputRow) => {
              val va = compiledA.eval(input)
              val vb = compiledB.eval(input)
              (va, vb) match {
                case (Num(a), Num(b)) => Bool(a > b).asInstanceOf[A]
                case (Date(a), Date(b)) => Bool(a.isAfter(b)).asInstanceOf[A]
                case (Str(a), Str(b)) => Bool(a > b).asInstanceOf[A]
                case (a, b) => throw new MatchError((a, op, b))
              }
            }

          case BinaryOp.GreaterEq => (input: InputRow) => {
              val va = compiledA.eval(input)
              val vb = compiledB.eval(input)
              (va, vb) match {
                case (Num(a), Num(b)) => Bool(a >= b).asInstanceOf[A]
                case (Date(a), Date(b)) => Bool(!b.isAfter(a)).asInstanceOf[A]
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
  
  def compileAssignment[A <: Value](assign: Checked.Assignment[A]): CompiledIO[Compiled.Assignment[A]] = {
    compileExpr(assign.expr).map { expr =>
      Compiled.Assignment(assign.name, expr)
    }
  }

}


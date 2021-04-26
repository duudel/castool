package castool.rql

import TokenKind.TokenKind

object Parser {
  case class Error(message: String, loc: Location) extends Serializable with Product

  sealed trait Result

  final case class Success(ast: Ast.Source) extends Result
  final case class Failure(errors: Seq[Error]) extends Result

  case class Input(current: Option[Token], tokens: Iterable[Token], pos: Int) {
    def hasInput: Boolean = current.nonEmpty
    def advance: Input = if (hasInput) {
      val nextOpt = tokens.headOption
      Input(nextOpt, if (tokens.nonEmpty) tokens.tail else tokens, nextOpt.map(_.pos).getOrElse(pos))
    } else {
      this
    }
  }

  object Input {
    def apply(tokens: Iterable[Token]): Input = {
      val currentOpt = tokens.headOption
      new Input(currentOpt, if (tokens.nonEmpty) tokens.tail else tokens, 0)
    }
  }

  case class Message(msg: String, pos: Int)

  sealed trait St[+A] extends Serializable with Product {
    def input: Input
    def result: A
    def isOk: Boolean
    def isError: Boolean
    def map[B](fn: A => B): St[B] = if (isOk) St.Ok(input, fn(result)) else this.asInstanceOf[St[B]]
  }
  object St {
    final case class Ok[+A](input: Input, result: A) extends St[A] {
      def isOk: Boolean = true
      def isError: Boolean = false
    }
    final case class Nok(input: Input, messages: Seq[Message], parserName: Option[String]) extends St[Nothing] {
      def result = throw new NoSuchElementException("Nok.result")
      def isOk: Boolean = false
      def isError: Boolean = false
      def toError: Error = Error(input, this)
    }
    final case class Error(input: Input, nok: Nok) extends St[Nothing] {
      def result = throw new NoSuchElementException("Error.result")
      def isOk: Boolean = false
      def isError: Boolean = true
    }

    def nok(input: Input, message: String, parserName: Option[String]): Nok =
      Nok(input, messages = Seq(Message(message, input.pos)), parserName = parserName)
  }

  type ParserFn[A,+B] = St[A] => St[B]
  trait Parser[+A] {
    def apply[B](st: St[B]): St[A]
    def andThen[B](p: Parser[B]): Parser[B] = Parser((apply _).andThen(p.apply _))
    def andThen[A1 >: A,B](pf: ParserFn[A1,B]): Parser[B] = Parser((apply _).andThen(pf))
    def named(n: String): Parser[A]
    def namedOpt(n: Option[String]): Parser[A]
    def name: Option[String]
  }

  object Parser {
    case class ParserImpl[A](name: Option[String], fn: St[Any] => St[A]) extends Parser[A] {
      def apply[B](st: St[B]): St[A] = fn(st)
      def named(n: String): Parser[A] = copy(name = Some(n))
      def namedOpt(n: Option[String]): Parser[A] = copy(name = n)
    }
    def apply[A,B](fn: ParserFn[A, B]): Parser[B] = ParserImpl(None, fn.asInstanceOf[St[Any] => St[B]])
  }

  object P {
    implicit class Pops[A](p: Parser[A]) {
      @inline def trace(name: String): Parser[A] = p /* Parser { st: St[Any] =>
        p(st) match {
          case ok: St.Ok[A] =>
            println(s"$name: ok ${ok.result}")
            ok
          case nok: St.Nok =>
            println(s"$name: nok ${nok.messages} - ${nok.parserName}")
            nok
          case err: St.Error =>
            println(s"$name: error ${err.nok.messages} - ${err.nok.parserName}")
            err
        }
      }*/
      @inline def fail(msg: String): Parser[Nothing] = Parser { st: St[Any] =>
        p(st) match {
          case ok: St.Ok[A] => St.nok(st.input, msg, p.name).toError
          case nok: St.Nok => nok
          case err: St.Error => err
        }
      }
      @inline def ~[B](other: Parser[B]): Parser[(A, B)] = Parser { (st: St[Any]) => {
        if (st.isError) st.map(_.asInstanceOf[(A, B)]) else {
          p(st) match {
            case ok: St.Ok[A] => other(ok).map(b => (ok.result, b))
            case nok: St.Nok   => nok
            case err: St.Error => err
          }
        }
      } }
      @inline def ~![B](other: Parser[B]): Parser[(A, B)] = Parser { (st: St[Any]) => {
        p(st) match {
          case ok: St.Ok[A] =>
            other(ok).map(b => (ok.result, b)) match {
              case nok: St.Nok   => nok.toError
              case err: St.Error => err
              case ok: St.Ok[(A, B)]  => ok
            }
          case nok: St.Nok => nok
          case err: St.Error => err
        }
      } }
      @inline def ~>![B](other: Parser[B]): Parser[B] = Parser { (st: St[Any]) => {
        p(st) match {
          case ok: St.Ok[A] =>
            other(ok) match {
              case nok: St.Nok   => nok.toError
              case err: St.Error => err
              case ok: St.Ok[B]  => ok
            }
          case nok: St.Nok => nok
          case err: St.Error => err
        }
      } }
      @inline def <~[B](other: Parser[B]): Parser[A] = Parser { (st: St[Any]) => {
        p(st) match {
          case ok: St.Ok[A] => other(ok).map(_ => ok.result)
          case nok: St.Nok => nok
          case err => err
        }
      } }
      @inline def ~>[B](other: Parser[B]): Parser[B] = Parser { (st: St[Any]) => {
        p(st) match {
          case ok: St.Ok[_] => other(ok)
          case nok: St.Nok => nok
          case err: St.Error => err
        }
      } }
      @inline def ~?[B](other: Parser[B]): Parser[(A, Option[B])] = p.andThen { stA: St[A] =>
        stA match {
          case St.Ok(inputA, a) =>
            other(stA) match {
              case ok: St.Ok[B] => ok.map(b => (a, Some(b)))
              case _: St.Nok => St.Ok(inputA, (a, None))
              case err: St.Error => err
            }
          case nok: St.Nok => nok
          case err: St.Error => err
        }
      }
      @inline def |[B](other: Parser[B]): Parser[_ >: A] = any(p, other)
      @inline def *(): Parser[Seq[A]] = many(p)
      @inline def *[S](separator: Parser[S]): Parser[Seq[A]] = manyWithSeparator(p, separator)
      @inline def +(): Parser[Seq[A]] = oneOrMore(p)
      @inline def +[S](separator: Parser[S]): Parser[Seq[A]] = oneOrMoreWithSeparator(p, separator)
      @inline def map[B](f: A => B): Parser[B] = Parser { (st: St[Any]) => {
        p(st).map(f)
      } }.namedOpt(p.name)
    }

    def accept[T <: Token](kind: TokenKind[T]): Parser[T] = Parser((st: St[Any]) => st match {
      case St.Ok(input, _) =>
        input.current.collect {
          case t: Token if t.kind == kind =>
            St.Ok(input.advance, t.asInstanceOf[T])
          case t =>
            St.nok(input, s"Expected '${kind.print}'", Some(s"'${kind.print}'"))
        }.getOrElse(St.nok(input, s"Expected '${kind.print}', but reached end of input", Some(s"'${kind.print}'")))
      case nok: St.Nok => nok
      case err: St.Error => err
    }).named(s"'${kind.print}'")

    def acceptIdent(s: String): Parser[Token.Ident] = {
      accept(TokenKind.ident).andThen { identSt: St[Token.Ident] =>
        identSt match {
          case St.Ok(_, result) if result.value == s =>
            identSt
          case St.Ok(input, result) => St.nok(input, s"Expected '$s'", Some(s"'$s'"))
          case nok: St.Nok => St.nok(nok.input, s"Expected '$s'", Some(s"'$s'"))
          case err: St.Error => err
        }
      }
    }.named(s"'$s'")

    def acceptName: Parser[NameAndToken] = {
      accept(TokenKind.ident).andThen { identSt: St[Token.Ident] =>
        identSt match {
          case St.Ok(input, result) =>
            Name.fromString(result.value) match {
              case Right(name) => St.Ok(input, NameAndToken(name, result))
              case Left(error) => St.nok(input, "Expected name", Some("name"))
            }
          case nok: St.Nok => nok
          case err: St.Error => err
        }
      }.named("name")
    }

    def orElse[A <: Token, B <: Token](a: Parser[A])(b: Parser[B]): Parser[Token] = Parser { (st: St[Any]) => a(st) match {
      case ok: St.Ok[A] => ok
      case nok: St.Nok => b(st)
      case err: St.Error => err
    } }

    def any[A](ps: Parser[A]*): Parser[A] = Parser { (st: St[Any]) =>
      var result: St[A] = null
      val nokPs = scala.collection.mutable.Buffer.empty[Parser[A]]
      val noks = scala.collection.mutable.Buffer.empty[St.Nok]
      val pIt = ps.iterator
      while (result == null && pIt.hasNext) {
        val p = pIt.next()
        p(st) match {
          case ok: St.Ok[A] => result = ok
          case err: St.Error => result = err
          case nok: St.Nok =>
            nokPs += p
            noks += nok
        }
      }
      if (result == null) {
        val anyOf = "any of " + noks.zip(nokPs).flatMap(a => a._1.parserName.orElse(a._2.name)).mkString(", ")
        St.nok(st.input, s"Expected ${anyOf}", Some(anyOf))
      } else {
        result
      }
    }

    def someOther[A](ps: Parser[A]*): Parser[Token] = Parser { st: St[Any] =>
      any(ps: _*)(st) match {
        case ok: St.Ok[A] =>
          val names = ps.flatMap(_.name)
          St.nok(ok.input, "Expecting other than" + names, Some("other than " + names))
        case nok: St.Nok if nok.input.hasInput => St.Ok(nok.input.advance, nok.input.current.get)
        case nok: St.Nok => nok
        case err: St.Error => err
      }
    }

    def many[A](p: Parser[A]): Parser[Seq[A]] = Parser { (st: St[Any]) => {
      if (st.isOk) {
        val results = scala.collection.mutable.Buffer.empty[A]
        var s = p(st)
        while (s.isOk) {
          results += s.result
          s = p(s)
        }
        s match {
          case err: St.Error => err
          case _ =>
            St.Ok(s.input, results.toSeq)
        }
      } else {
        st.map(_ => Nil)
      }
    } }

    def manyWithSeparator[A, S](p: Parser[A], separator: Parser[S]): Parser[Seq[A]] = Parser { (st: St[Any]) => {
      val results = scala.collection.mutable.Buffer.empty[A]
      var s = p(st)
      while (s.isOk) {
        results += s.result
        s = separator(s).map(_.asInstanceOf[A])
        if (s.isOk) {
          s = p(s)
        }
      }
      s match {
        case err: St.Error => err
        case _ => St.Ok(s.input, results.toSeq)
      }
    } }

    def oneOrMore[A](p: Parser[A]): Parser[Seq[A]] = Parser { (st: St[Any]) => {
      def step(state: St[_], results: Vector[A]): St[Vector[A]] = {
        p(state) match {
          case ok @ St.Ok(_, item) => step(ok, results :+ item)
          case nok: St.Nok         => state.map(_ => results)
          case err: St.Error       => err
        }
      }
      p(st) match {
        case ok @ St.Ok(_, item) =>
          step(ok, Vector(item))
        case nok: St.Nok   => nok
        case err: St.Error => err
      }
    } }

    def oneOrMoreWithSeparator[A, S](p: Parser[A], separator: Parser[S]): Parser[Seq[A]] = Parser { (st: St[Any]) => {
      def step(state: St[_], results: Vector[A]): St[Vector[A]] = {
        p(state) match {
          case ok @ St.Ok(_, item) =>
            separator(ok) match {
              case sepOk: St.Ok[_] => step(sepOk, results :+ item)
              case nok: St.Nok     => ok.map(_ => results :+ item)
              case err: St.Error   => err
            }
          case nok: St.Nok   => nok
          case err: St.Error => err
        }
      }
      step(st, Vector.empty)
    } }

    def logical_expr[OpTok <: Token.LogicalOp](opKind: TokenKind[OpTok])(subExpr: Parser[Ast.Expr]): Parser[Ast.Expr] =
      (subExpr ~ many(accept(opKind) ~! subExpr)).map {
        case (e0, Nil) =>
          e0
        case (left, (opToken, right) :: rest) =>
          def build(left: Ast.Expr, opToken: Token.LogicalOp, right: Ast.Expr, rest: Seq[(Token.LogicalOp, Ast.Expr)]): Ast.Expr = {
            val op = opToken.binaryOp
            rest match {
              case Nil =>
                Ast.BinaryExpr(op = op, exprA = left, exprB = right, pos = opToken.pos)
              case (nextOp, nextRight) :: tail =>
                val nextLeft = Ast.BinaryExpr(op = op, exprA = left, exprB = right, pos = opToken.pos)
                build(nextLeft, nextOp, nextRight, tail)
            }
          }
          build(left, opToken, right, rest)
        case x => throw new MatchError("Invalid parsed expr " + x)
      }

    def arith2_expr[
      T1 <: Token.ArithmeticOp,
      T2 <: Token.ArithmeticOp
    ](opKind1: TokenKind[T1], opKind2: TokenKind[T2])(subExpr: Parser[Ast.Expr]): Parser[Ast.Expr] =
      (subExpr ~ many(any(accept(opKind1), accept(opKind2)) ~! subExpr)).map {
        case (e0, Nil) =>
          e0
        case (left, (opToken, right) :: rest) =>
          def build(left: Ast.Expr, opToken: Token.ArithmeticOp, right: Ast.Expr, rest: Seq[(Token.ArithmeticOp, Ast.Expr)]): Ast.Expr = {
            val op = opToken.binaryOp
            rest match {
              case Nil =>
                Ast.BinaryExpr(op = op, exprA = left, exprB = right, pos = opToken.pos)
              case (nextOp, nextRight) :: tail =>
                val nextLeft = Ast.BinaryExpr(op = op, exprA = left, exprB = right, pos = opToken.pos)
                build(nextLeft, nextOp, nextRight, tail)
            }
          }
          build(left, opToken, right, rest)
        case x => throw new MatchError("Invalid parsed expr " + x)
      }

    def bin2_expr[
      T1 <: Token.BinOpToken: TokenKind,
      T2 <: Token.BinOpToken: TokenKind
    ](opKind1: TokenKind[T1], opKind2: TokenKind[T2])(subExpr: Parser[Ast.Expr]): Parser[Ast.Expr] =
      (subExpr ~ many(any(accept(opKind1), accept(opKind2)) ~! subExpr)).map {
        case (e0, Nil) =>
          e0
        case (left, (opToken, right) :: rest) =>
          def build(left: Ast.Expr, opToken: Token.BinOpToken, right: Ast.Expr, rest: Seq[(Token.BinOpToken, Ast.Expr)]): Ast.Expr = {
            val op = opToken.binaryOp
            rest match {
              case Nil =>
                Ast.BinaryExpr(op = op, exprA = left, exprB = right, pos = opToken.pos)
              case (nextOp, nextRight) :: tail =>
                val nextLeft = Ast.BinaryExpr(op = op, exprA = left, exprB = right, pos = opToken.pos)
                build(nextLeft, nextOp, nextRight, tail)
            }
          }
          build(left, opToken, right, rest)
        case x => throw new MatchError("Invalid parsed expr " + x)
      }

    def bin_expr[T <: Token.BinOpToken](subExpr: Parser[Ast.Expr], opParser: Parser[T]): Parser[Ast.Expr] =
      (subExpr ~ many(opParser ~! subExpr)).map {
        case (e0, Nil) =>
          e0
        case (left, (opToken, right) :: rest) =>
          def build(left: Ast.Expr, opToken: Token.BinOpToken, right: Ast.Expr, rest: Seq[(Token.BinOpToken, Ast.Expr)]): Ast.Expr = {
            val op = opToken.binaryOp
            rest match {
              case Nil =>
                Ast.BinaryExpr(op = op, exprA = left, exprB = right, pos = opToken.pos)
              case (nextOp, nextRight) :: tail =>
                val nextLeft = Ast.BinaryExpr(op = op, exprA = left, exprB = right, pos = opToken.pos)
                build(nextLeft, nextOp, nextRight, tail)
            }
          }
          build(left, opToken, right, rest)
        case x => throw new MatchError("Invalid parsed expr " + x)
      }

  }

  object G {
    import P._

    lazy val column_expr: Parser[Ast.Column] = acceptName.map(nameAndTok => Ast.Column(nameAndTok.name, nameAndTok.tok.pos))
    lazy val null_lit_expr: Parser[Ast.NullLit] = accept(TokenKind.nullKind).map(lit => Ast.NullLit(lit.pos))
    lazy val true_lit_expr: Parser[Ast.TrueLit] = accept(TokenKind.trueKind).map(lit => Ast.TrueLit(lit.pos))
    lazy val false_lit_expr: Parser[Ast.FalseLit] = accept(TokenKind.falseKind).map(lit => Ast.FalseLit(lit.pos))
    lazy val number_lit_expr: Parser[Ast.NumberLit] = accept(TokenKind.number).map(lit => Ast.NumberLit(lit))
    lazy val date_lit_expr: Parser[Ast.DateLit] = accept(TokenKind.date).map(lit => Ast.DateLit(lit))
    lazy val string_lit_expr: Parser[Ast.StringLit] = accept(TokenKind.string).map(lit => Ast.StringLit(lit))
    lazy val funcCall_expr: Parser[Ast.FunctionCall] =
      ((acceptName <~ accept(TokenKind.lparen)) ~ Parser((st: St[Any]) => expr(st)).*(accept(TokenKind.comma)) <~ accept(TokenKind.rparen)).map {
        case (nameAndTok, args) => Ast.FunctionCall(functionName = nameAndTok.name, args = args, pos = nameAndTok.tok.pos)
      }
    lazy val parenth_expr: Parser[_ <: Ast.Expr] = (accept(TokenKind.lparen) ~> Parser((st: St[Any]) => expr(st)) <~ accept(TokenKind.rparen))
    lazy val term_expr: Parser[_ <: Ast.Expr] = any(
      funcCall_expr, column_expr, null_lit_expr, true_lit_expr, false_lit_expr, number_lit_expr, string_lit_expr, date_lit_expr, parenth_expr
    )
    lazy val unary_expr: Parser[_ <: Ast.Expr] = (any(accept(TokenKind.not), accept(TokenKind.minus), accept(TokenKind.plus)) ~! term_expr).map {
      case (opToken, expr) => Ast.UnaryExpr(op = opToken.unaryOp, expr = expr, pos = opToken.pos)
    }
    lazy val mult_expr: Parser[_ <: Ast.Expr] = arith2_expr(TokenKind.multiply, TokenKind.divide)(any(unary_expr, term_expr))
    lazy val sum_expr: Parser[_ <: Ast.Expr] = arith2_expr(TokenKind.plus, TokenKind.minus)(mult_expr)
    lazy val comp_ltgt_expr: Parser[_ <: Ast.Expr] = bin_expr(sum_expr, any(
      accept(TokenKind.less), accept(TokenKind.lessEq), accept(TokenKind.greater), accept(TokenKind.greaterEq))
    )
    lazy val comp_expr: Parser[_ <: Ast.Expr] = bin_expr(comp_ltgt_expr, any(accept(TokenKind.eq), accept(TokenKind.notEq)))
    lazy val string_expr: Parser[_ <: Ast.Expr] = bin2_expr(TokenKind.contains, TokenKind.notContains)(comp_expr)
    lazy val logical_and_expr: Parser[_ <: Ast.Expr] = logical_expr(TokenKind.and)(string_expr)
    lazy val logical_or_expr: Parser[_ <: Ast.Expr] = logical_expr(TokenKind.or)(logical_and_expr)
    lazy val expr: Parser[_ <: Ast.Expr] = logical_or_expr.trace("expr")

    lazy val table: Parser[Ast.Table] = acceptName.map(nameAndTok => Ast.Table(name = nameAndTok.name, tok = nameAndTok.tok))
    lazy val where: Parser[Ast.Where] = (acceptIdent("where") ~! expr).map { case (w, expr) => Ast.Where(expr, w.pos) }.trace("where")
    lazy val extend: Parser[Ast.Extend] = (acceptIdent("extend") ~! acceptName ~ (accept(TokenKind.assign) ~> expr)).map {
      case ((e, nameAndTok), expr) => Ast.Extend(name = nameAndTok, expr = expr, pos = e.pos)
    }
    lazy val project: Parser[Ast.Project] = (acceptIdent("project") ~! acceptName.*(accept(TokenKind.comma))).map {
      case (p, names) => Ast.Project(names, p.pos)
    }
    lazy val orderBy: Parser[Ast.OrderBy] = ((acceptIdent("order") ~! acceptIdent("by"))
      ~! acceptName.+(accept(TokenKind.comma))
      ~? (any(acceptIdent("asc"), acceptIdent("desc")) | someOther(accept(TokenKind.bar)).fail("Expected 'asc' or 'desc'"))).map { case (((o, _), names), orderIdent) =>
        val order = orderIdent match {
          case Some(o) => o match {
            case ident: Token.Ident if ident.value == "asc" =>
              Order.Asc
            case _ =>
              Order.Desc
          }
          case None => Order.Asc
        }
        Ast.OrderBy(names, order, pos = o.pos)
      }
    lazy val summarize: Parser[Ast.Summarize] = (acceptIdent("summarize")
      ~! (acceptName ~ (accept(TokenKind.assign) ~> funcCall_expr))
          .+(accept(TokenKind.comma))
      ~? (acceptIdent("by") ~ acceptName.*(accept(TokenKind.comma)))).map {
        case ((s, aggrs), groupByOpt) =>
          val aggregations = aggrs.map { case (nameAndTok, aggr) => Ast.Aggregation(nameAndTok, aggr) }
          val groupBy = groupByOpt.map(_._2).getOrElse(Seq.empty)
          Ast.Summarize(aggregations, groupBy, pos = s.pos)
      }
    lazy val toplevel: Parser[Ast.Source] = (table
      ~? (accept(TokenKind.bar) ~>! any(where, extend, project, orderBy, summarize).+(accept(TokenKind.bar)))
    ).map {
      case (table, None) => table
      case (table, Some(ops)) => ops.foldLeft[Ast.Source](table) {
        case (acc, op) => Ast.Cont(acc, op)
      }
    }.trace("toplevel")
  }

  def parseWith[A](tokens: Iterable[Token], parser: Parser[A]): St[A] = {
    val state = St.Ok(Input(tokens), None)
    parser(state)
  }

  def parse[A](tokens: Iterable[Token]): Result = {
    parseWith(tokens, G.toplevel) match {
      case St.Ok(Input(None, _, _), ast) => Success(ast)
      case St.Ok(Input(Some(current), _, pos), ast) =>
        val error = Error("Unrecognized input after succesfully parsed query: " + current.display, Location(pos))
        Failure(Seq(error))
      case St.Nok(input, messages, parserName) =>
        val errors = messages.map(msg => Error("Expected " + msg, Location(input.pos)))
        Failure(errors)
      case St.Error(input, nok) =>
        val errors = nok.messages.map(msg => Error(msg.msg, Location(msg.pos)))
        Failure(errors)
    }
  }

}


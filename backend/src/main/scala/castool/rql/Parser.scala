package castool.rql

import scala.reflect.ClassTag

object Parser {
  case class Error(message: String, loc: Location) extends Serializable with Product

  sealed trait Result

  final case class Success(ast: Ast.Source) extends Result
  final case class Failure(errors: Seq[Error]) extends Result

  case class Input(current: Option[Token], tokens: Iterable[Token]) {
    def hasInput: Boolean = current.nonEmpty
    def advance: Input = if (hasInput) {
      val nextOpt = tokens.headOption
      Input(nextOpt, if (tokens.nonEmpty) tokens.tail else tokens)
    } else {
      this
    }
  }

  object Input {
    def apply(tokens: Iterable[Token]): Input = {
      val currentOpt = tokens.headOption
      new Input(currentOpt, if (tokens.nonEmpty) tokens.tail else tokens)
    }
  }

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
    final case class Nok(input: Input, messages: Seq[String]) extends St[Nothing] {
      def result = throw new NoSuchElementException("Nok.result")
      def isOk: Boolean = false
      def isError: Boolean = false
    }
    final case class Error(input: Input, messages: Seq[String]) extends St[Nothing] {
      def result = throw new NoSuchElementException("Error.result")
      def isOk: Boolean = false
      def isError: Boolean = true
    }
  }

  type Parser[A] = St[Any] => St[A]

  object P {
    implicit class Pops[A](p: Parser[A]) {
      @inline def mapNok(f: St.Nok => St[A]): Parser[A] = p.andThen {
        case nok: St.Nok => f(nok)
        case x           => x
      }
      @inline def ~[B](other: Parser[B]): Parser[(A, B)] = (st: St[Any]) => {
        if (st.isError) st.map(_.asInstanceOf[(A, B)]) else {
          p(st) match {
            case ok: St.Ok[_] => other(ok).map(b => (ok.result, b))
            case nok: St.Nok => nok
            case err: St.Error => err
          }
        }
      }
      @inline def ~![B](other: Parser[B]): Parser[(A, B)] = (st: St[Any]) => {
        p(st) match {
          case ok: St.Ok[_] =>
            other(ok).map(b => (ok.result, b)) match {
              case nok: St.Nok =>
                println("~! nok: " + nok)
                St.Error(nok.input, nok.messages)
              case err: St.Error =>
                println("~! err: " + err)
                err
              case x: St.Ok[_] =>
                println("~! ok input: " + x.input)
                println("   ok result: " + x.result)
                x
            }
          case nok: St.Nok => nok
          case err: St.Error => err
        }
      }
      @inline def <~[B](other: Parser[B]): Parser[A] = (st: St[Any]) => {
        p(st) match {
          case ok: St.Ok[_] => other(ok).map(_ => ok.result)
          case nok: St.Nok => nok
          case err => err
        }
      }
      @inline def ~>[B](other: Parser[B]): Parser[B] = (st: St[Any]) => {
        p(st) match {
          case ok: St.Ok[_] => other(ok)
          case nok: St.Nok => nok
          case err: St.Error => err
        }
      }
      @inline def ~?[B](other: Parser[B]): Parser[(A, Option[B])] = p.andThen { stA =>
        stA match {
          case St.Ok(inputA, a) =>
            other(stA) match {
              case ok: St.Ok[_] => ok.map(b => (a, Some(b)))
              case _: St.Nok => St.Ok(inputA, (a, None))
              case err: St.Error => err
            }
          case nok: St.Nok => nok
          case err: St.Error => err
        }
      }
      @inline def *(): Parser[Seq[A]] = many(p)
      @inline def *[S](separator: Parser[S]): Parser[Seq[A]] = manyWithSeparator(p, separator)
      @inline def *[S1, S2](sep1: Parser[S1], sep2: Parser[S2]): Parser[Seq[A]] = manyWithSeparator(p, sep1, sep2)
      @inline def +(): Parser[Seq[A]] = oneOrMore(p)
      @inline def +[S](separator: Parser[S]): Parser[Seq[A]] = oneOrMoreWithSeparator(p, separator)
      @inline def +[S1, S2](sep1: Parser[S1], sep2: Parser[S2]): Parser[Seq[A]] = oneOrMoreWithSeparator(p, sep1, sep2)
      @inline def map[B](f: A => B): Parser[B] = (st: St[Any]) => {
        p(st).map(f)
      }
    }

    def accept[T <: Token](implicit tc: ClassTag[T]): Parser[T] = {
      val tclass = tc.runtimeClass
      val tokenKind = tclass.getSimpleName()

      (st: St[Any]) => st match {
        case St.Ok(input, _) =>
          input.current.collect {
            case t: Token if t.getClass() == tclass =>
              St.Ok(input.advance, t.asInstanceOf[T])
            case t =>
              St.Nok(input, Seq(s"Expected $tokenKind, got '${t.display}'"))
          }.getOrElse(St.Nok(input, Seq(s"Expected $tokenKind, but reached end of input")))
        case nok: St.Nok => nok.map(_.asInstanceOf[T])
        case err: St.Error => err.map(_.asInstanceOf[T])
      }
    }

    def acceptIdent(s: String): Parser[Token.Ident] = {
      accept[Token.Ident].andThen(identSt => {
        identSt match {
          case St.Ok(_, result) if result.value == s =>
            identSt
          case St.Ok(input, result) => St.Nok(input, Seq(s"Expected '$s', got ${result.value}"))
          case nok: St.Nok => St.Nok(nok.input, Seq(s"Expected '$s'"))
          case err: St.Error => St.Error(err.input, Seq(s"Expected '$s'"))
        }
      })
    }

    def acceptName: Parser[NameAndToken] = {
      accept[Token.Ident].andThen(identSt => {
        identSt match {
          case St.Ok(input, result) =>
            Name.fromString(result.value) match {
              case Right(name) => St.Ok(input, NameAndToken(name, result))
              case Left(error) => St.Nok(input, Seq(s"Expected name identified: $error"))
            }
          case nok: St.Nok => St.Nok(nok.input, Seq(s"Expected name identifier"))
          case err: St.Error => St.Error(err.input, Seq(s"Expected name identifier"))
        }
      })
    }

    def orElse[A <: Token, B <: Token](a: Parser[A])(b: Parser[B]): Parser[Token] = (st: St[Any]) => a(st) match {
      case ok: St.Ok[_] => ok
      case nok => b(st)
    }

    def any[A](ps: Parser[A]*): Parser[A] = (st: St[Any]) => {
      var result: St[A] = null
      val noks = scala.collection.mutable.Buffer.empty[St.Nok]
      val pIt = ps.iterator
      while (result == null && pIt.hasNext) {
        val p = pIt.next()
        p(st) match {
          case ok: St.Ok[A] => result = ok
          case err: St.Error => result = err
          case nok: St.Nok => noks += nok
        }
      }
      if (result == null) {
        St.Nok(st.input, Seq("Did not match any") ++ noks.flatMap(_.messages))
      } else {
        result
      }
    }

    def many[A](p: Parser[A]): Parser[Seq[A]] = (st: St[Any]) => {
      assert(st.isOk)
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
    }

    def manyWithSeparator[A, S](p: Parser[A], separator: Parser[S]): Parser[Seq[A]] = (st: St[Any]) => {
      assert(st.isOk)

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
        case _ =>
          St.Ok(s.input, results.toSeq)
      }
    }

    def manyWithSeparator[A, S1, S2](p: Parser[A], sep1: Parser[S1], sep2: Parser[S2]): Parser[Seq[A]] = sep1 ~> ((st: St[Any]) => {
      assert(st.isOk)

      val results = scala.collection.mutable.Buffer.empty[A]
      var s = p(st)
      while (s.isOk) {
        results += s.result
        s = sep2(s).map(_.asInstanceOf[A])
        if (s.isOk) {
          s = p(s)
        }
      }
      s match {
        case err: St.Error => err
        case _ =>
          St.Ok(s.input, results.toSeq)
      }
    })

    def oneOrMore[A](p: Parser[A]): Parser[Seq[A]] = (st: St[Any]) => {
      var s = p(st)
      if (!s.isOk) {
        s.map(_ => Nil)
      } else {
        val results = scala.collection.mutable.Buffer.empty[A]
        while (s.isOk) {
          results += s.result
          s = p(s)
        }
        s match {
          case err: St.Error => err
          case _             => St.Ok(s.input, results.toSeq)
        }
      }
    }

    def oneOrMoreWithSeparator[A, S](p: Parser[A], separator: Parser[S]): Parser[Seq[A]] = (st: St[Any]) => {
      var s = p(st)
      if (!s.isOk) {
        s.map(_ => Nil)
      } else {
        val results = scala.collection.mutable.Buffer.empty[A]
        while (s.isOk) {
          results += s.result

          s = separator(s).map(_.asInstanceOf[A])
          if (s.isOk) {
            s = p(s)
          }
        }
        s match {
          case err: St.Error => err
          case _ =>
            St.Ok(s.input, results.toSeq)
        }
      }
    }

    def oneOrMoreWithSeparator[A, S1, S2](p: Parser[A], sep1: Parser[S1], sep2: Parser[S2]): Parser[Seq[A]] = sep1 ~> ((st: St[Any]) => {
      var s = p(st)
      if (!s.isOk) {
        s.map(_ => Nil)
      } else {
        val results = scala.collection.mutable.Buffer.empty[A]
        while (s.isOk) {
          results += s.result

          s = sep2(s).map(_.asInstanceOf[A])
          if (s.isOk) {
            s = p(s)
          }
        }
        s match {
          case err: St.Error => err
          case _ =>
            St.Ok(s.input, results.toSeq)
        }
      }
    })

    def logical_expr[OpTok <: Token.LogicalOp: ClassTag](subExpr: Parser[Ast.Expr]): Parser[Ast.Expr] =
      (subExpr ~ many(accept[OpTok] ~ subExpr)).map {
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
      T1 <: Token.ArithmeticOp: ClassTag,
      T2 <: Token.ArithmeticOp: ClassTag
    ](subExpr: Parser[Ast.Expr]): Parser[Ast.Expr] =
      (subExpr ~ many(any(accept[T1], accept[T2]) ~ subExpr)).map {
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
      T1 <: Token.BinOpToken: ClassTag,
      T2 <: Token.BinOpToken: ClassTag
    ](subExpr: Parser[Ast.Expr]): Parser[Ast.Expr] =
      (subExpr ~ many(any(accept[T1], accept[T2]) ~! subExpr)).map {
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

    def bin_expr[T <: Token.BinOpToken: ClassTag](subExpr: Parser[Ast.Expr], opParser: Parser[T]): Parser[Ast.Expr] =
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
    lazy val null_lit_expr: Parser[Ast.NullLit] = acceptIdent("null").map(ident => Ast.NullLit(ident.pos))
    lazy val true_lit_expr: Parser[Ast.TrueLit] = acceptIdent("true").map(ident => Ast.TrueLit(ident.pos))
    lazy val false_lit_expr: Parser[Ast.FalseLit] = acceptIdent("false").map(ident => Ast.FalseLit(ident.pos))
    lazy val number_lit_expr: Parser[Ast.NumberLit] = accept[Token.NumberLit].map(lit => Ast.NumberLit(lit))
    lazy val string_lit_expr: Parser[Ast.StringLit] = accept[Token.StringLit].map(lit => Ast.StringLit(lit))
    lazy val funcCall_expr: Parser[Ast.FunctionCall] =
      ((acceptName <~ accept[Token.LParen]) ~ (st => expr(st)).*(accept[Token.Comma]) <~ accept[Token.RParen]).map {
        case (nameAndTok, args) => Ast.FunctionCall(functionName = nameAndTok.name, args = args, pos = nameAndTok.tok.pos)
      }
    lazy val parenth_expr: Parser[_ <: Ast.Expr] = (accept[Token.LParen] ~> (st => expr(st)) <~ accept[Token.RParen])
    lazy val term_expr: Parser[_ <: Ast.Expr] = any(
      funcCall_expr, column_expr, null_lit_expr, true_lit_expr, false_lit_expr, number_lit_expr, string_lit_expr, parenth_expr
    )
    lazy val unary_expr: Parser[_ <: Ast.Expr] = (any(accept[Token.OpNot], accept[Token.OpPlus], accept[Token.OpMinus]) ~ term_expr).map {
      case (opToken, expr) => Ast.UnaryExpr(op = opToken.unaryOp, expr = expr, pos = opToken.pos)
    }
    lazy val mult_expr: Parser[_ <: Ast.Expr] = arith2_expr[Token.OpMultiply, Token.OpDivide](any(unary_expr, term_expr))
    lazy val sum_expr: Parser[_ <: Ast.Expr] = arith2_expr[Token.OpPlus, Token.OpMinus](mult_expr)
    lazy val comp_ltgt_expr: Parser[_ <: Ast.Expr] = bin_expr(sum_expr, any(
      accept[Token.OpLess], accept[Token.OpLessEq], accept[Token.OpGreater], accept[Token.OpGreaterEq])
    )
    lazy val comp_expr: Parser[_ <: Ast.Expr] = bin_expr(comp_ltgt_expr, any(accept[Token.OpEqual], accept[Token.OpNotEqual]))
    lazy val string_expr: Parser[_ <: Ast.Expr] = bin2_expr[Token.OpContains, Token.OpNotContains](comp_expr)
    lazy val logical_and_expr: Parser[_ <: Ast.Expr] = logical_expr[Token.OpAnd](string_expr)
    lazy val logical_or_expr: Parser[_ <: Ast.Expr] = logical_expr[Token.OpOr](logical_and_expr)
    lazy val expr: Parser[_ <: Ast.Expr] = logical_or_expr

    lazy val table: Parser[Ast.Table] = acceptName.map(nameAndTok => Ast.Table(name = nameAndTok.name, tok = nameAndTok.tok))
    lazy val where: Parser[Ast.Where] = (acceptIdent("where") ~! expr).map { case (w, expr) => Ast.Where(expr, w.pos) }
    lazy val extend: Parser[Ast.Extend] = (acceptIdent("extend") ~! acceptName ~ (accept[Token.OpAssign] ~> expr)).map {
      case ((e, nameAndTok), expr) => Ast.Extend(name = nameAndTok, expr = expr, pos = e.pos)
    }
    lazy val project: Parser[Ast.Project] = (acceptIdent("project") ~! acceptName.*(accept[Token.Comma])).map {
      case (p, names) => Ast.Project(names, p.pos)
    }
    lazy val orderBy: Parser[Ast.OrderBy] = ((acceptIdent("order") ~! acceptIdent("by"))
      ~! acceptName.*(accept[Token.Comma])
      ~? any(acceptIdent("asc"), acceptIdent("desc"))).map { case (((o, _), names), orderIdent) =>
        val order = orderIdent match {
          case Some(o) if o.value == "asc" => Order.Asc
          case Some(o)                     => Order.Desc
          case None                        => Order.Asc
        }
        Ast.OrderBy(names, order, pos = o.pos)
      }
      lazy val summarize: Parser[Ast.Summarize] = (acceptIdent("summarize")
        ~! (acceptName ~ (accept[Token.OpAssign] ~> funcCall_expr))
            .+(accept[Token.Comma])
            .mapNok(nok => St.Nok(nok.input, Seq(s"summarize expects assignments of aggregation functions, but parsing failed with:") ++ nok.messages))
        ~? (acceptIdent("by") ~ acceptName.*(accept[Token.Comma]))).map {
          case ((s, aggrs), groupByOpt) =>
            val aggregations = aggrs.map { case (nameAndTok, aggr) => Ast.Aggregation(nameAndTok, aggr) }
            val groupBy = groupByOpt.map(_._2).getOrElse(Seq.empty)
            Ast.Summarize(aggregations, groupBy, pos = s.pos)
        }
      lazy val toplevel: Parser[Ast.Source] = (table
        ~? any[Ast.TopLevelOp](where, extend, project, orderBy, summarize).*(accept[Token.Bar], accept[Token.Bar])).map {
          case (table, None) => table
          case (table, Some(ops)) => ops.foldLeft[Ast.Source](table) {
            case (acc, op) => Ast.Cont(acc, op)
          }
      }
  }

  def parseWith[A](tokens: Iterable[Token], parser: Parser[A]): St[A] = {
    val state = St.Ok(Input(tokens), None)
    parser(state)
  }

  def parse[A](tokens: Iterable[Token]): Result = {
    parseWith(tokens, G.toplevel) match {
      case St.Ok(Input(None, _), ast) => Success(ast)
      case St.Ok(Input(Some(current), _), ast) =>
        val error = Error("Unrecognized input after succesfully parsed query: " + current.display, Location(current.pos))
        Failure(Seq(error))
      case St.Nok(input, messages) =>
        val location = if (input.hasInput) Location(input.current.get.pos) else Location(-1)
        val errors = messages.map(msg => Error(msg, location))
        Failure(errors)
      case St.Error(input, messages) =>
        val location = if (input.hasInput) Location(input.current.get.pos) else Location(-1)
        val errors = messages.map(msg => Error(msg, location))
        Failure(errors)
    }
  }

}


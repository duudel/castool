package castool.rql

import scala.reflect.ClassTag

object Parser {
  sealed trait Result

  final case class Success(ast: Ast.Source) extends Result
  final case class Failure(error: String, pos: Int) extends Result

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
    final case class Nok(input: Input, message: String) extends St[Nothing] {
      def result = throw new NoSuchElementException("Nok.result")
      def isOk: Boolean = false
      def isError: Boolean = false
    }
    final case class Error(input: Input, message: String) extends St[Nothing] {
      def result = throw new NoSuchElementException("Error.result")
      def isOk: Boolean = false
      def isError: Boolean = true
    }
  }

  type Parser[A] = St[Any] => St[A]

  object P {
    implicit class Pops[A](p: Parser[A]) {
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
              case nok: St.Nok => St.Error(nok.input, nok.message)
              case x => x
            }
          case nok: St.Nok => St.Error(nok.input, nok.message)
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
              //println(s"Match $t")
              St.Ok(input.advance, t.asInstanceOf[T])
            case t =>
              St.Nok(input, s"Expected $tokenKind, got ${t.getClass.getSimpleName}")
          }.getOrElse(St.Nok(input, s"Expected $tokenKind, but reached end of input"))
        case nok: St.Nok => nok.map(_.asInstanceOf[T])
        case err: St.Error => err.map(_.asInstanceOf[T])
      }
    }

    def acceptIdent(s: String): Parser[Token.Ident] = {
      accept[Token.Ident].andThen(identSt => {
        identSt match {
          case St.Ok(_, result) if result.value == s =>
            identSt
          case St.Ok(input, result) => St.Nok(input, s"Expected '$s', got ${result.value}")
          case nok: St.Nok => St.Nok(nok.input, s"Expected '$s'")
          case err: St.Error => St.Error(err.input, s"Expected '$s'")
        }
      })
    }

    def acceptName: Parser[NameAndToken] = {
      accept[Token.Ident].andThen(identSt => {
        identSt match {
          case St.Ok(input, result) =>
            Name.fromString(result.value) match {
              case Right(name) => St.Ok(input, NameAndToken(name, result))
              case Left(error) => St.Nok(input, error)
            }
          case nok: St.Nok => St.Nok(nok.input, s"Expected name")
          case err: St.Error => St.Error(err.input, s"Expected name")
        }
      })
    }

    def orElse[A <: Token, B <: Token](a: Parser[A])(b: Parser[B]): Parser[Token] = (st: St[Any]) => a(st) match {
      case ok: St.Ok[_] => ok
      case nok => b(st)
    }

    def any[A](ps: Parser[A]*): Parser[A] = (st: St[Any]) => {
      var result: St.Ok[A] = null
      ps.foreach { p =>
        p(st) match {
          case ok: St.Ok[A] if result == null => result = ok
          case _ =>
        }
      }
      if (result == null) St.Nok(st.input, "Did not match any") else result
      //.map(p => p(st))
      //.find(s => s.isOk || s.isError)
      ////.map {
      ////  case ok: St.Ok[A] => ok
      ////  case error: St.Error => error
      ////}
      //.getOrElse(St.Nok(st.input, "no match for any"))
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
        St.Ok(s.input, results.toSeq)
      } else {
        st.map(_ => Nil)
      }
    }

    def manyWithSeparator[A, S](p: Parser[A], separator: Parser[S]): Parser[Seq[A]] = (st: St[Any]) => {
      assert(st.isOk)

      val results = scala.collection.mutable.Buffer.empty[A]
      var s = p(st)
      while (s.isOk) {
        //println(s"* $s")
        results += s.result
        s = separator(s).map(_.asInstanceOf[A])
        //println(s"* $s")
        if (s.isOk) {
          s = p(s)
        }
      }
      St.Ok(s.input, results.toSeq)
    }

    def manyWithSeparator[A, S1, S2](p: Parser[A], sep1: Parser[S1], sep2: Parser[S2]): Parser[Seq[A]] = sep1 ~> ((st: St[Any]) => {
      assert(st.isOk)

      val results = scala.collection.mutable.Buffer.empty[A]
      var s = p(st)
      while (s.isOk) {
        //println(s"* $s")
        results += s.result
        s = sep2(s).map(_.asInstanceOf[A])
        //println(s"* $s")
        if (s.isOk) {
          s = p(s)
        }
      }
      St.Ok(s.input, results.toSeq)
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
      ((acceptName <~ accept[Token.LParen]) ~ ((st) => expr(st)).*(accept[Token.Comma]) <~ accept[Token.RParen]).map {
        case (nameAndTok, args) => Ast.FunctionCall(functionName = nameAndTok.name, args = args, pos = nameAndTok.tok.pos)
      }
    lazy val term_expr: Parser[_ <: Ast.Expr] = any(
      funcCall_expr, column_expr, null_lit_expr, true_lit_expr, false_lit_expr, number_lit_expr, string_lit_expr
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
    lazy val orderBy: Parser[Ast.OrderBy] = ((acceptIdent("order") <~ acceptIdent("by"))
      ~! acceptName.*(accept[Token.Comma])
      ~ any(acceptIdent("asc"), acceptIdent("desc"))).map { case ((o, names), orderIdent) =>
        val order = if (orderIdent.value == "asc") Order.Asc else Order.Desc
        Ast.OrderBy(names, order, pos = o.pos)
      }
      lazy val summarize: Parser[Ast.Summarize] = (acceptIdent("summarize")
        ~! (acceptName ~ (accept[Token.OpAssign] ~> funcCall_expr)).*(accept[Token.Comma])
        ~? (acceptIdent("by") ~ acceptName.*(accept[Token.Comma]))).map {
          case ((s, aggrs), groupByOpt) =>
            val aggregations = aggrs.map { case (nameAndTok, aggr) => Ast.Aggregation(nameAndTok, aggr) }
            val groupBy = groupByOpt.map(_._2).getOrElse(Seq.empty)
            Ast.Summarize(aggregations, groupBy, pos = s.pos)
        }
      lazy val toplevel: Parser[Ast.Source] = (table
        ~ any[Ast.TopLevelOp](where, extend, project, orderBy, summarize).*(accept[Token.Bar], accept[Token.Bar])).map {
          case (table, Nil) => table
          case (table, ops) => ops.foldLeft[Ast.Source](table) {
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
      case St.Ok(Input(Some(current), _), ast) => Failure("Unrecognized input after succesfully parsed query: " + current.display, current.pos)
      case St.Nok(input, message) => Failure(message, if (input.hasInput) input.current.get.pos else -1)
      case St.Error(input, message) => Failure(message, if (input.hasInput) input.current.get.pos else -1)
    }
  }

  def main(): Unit = {
    Lexer.lex("TableName or What and that or theOther") match {
      case Lexer.Success(tokens) =>
        val result = parseWith(tokens, G.expr)
        println(result)
      case Lexer.Failure(error, _) => println("Error " + error)
    }

    Lexer.lex("func(arg0+") match {
      case Lexer.Success(tokens) =>
        val result = parseWith(tokens, G.expr)
        println(result)
      case Lexer.Failure(error, _) => println("Error " + error)
    }

    Lexer.lex("""
      Rows | where persistence_id contains "kala"
      | extend json=as_json(event)
      | summarize cnt=count() by persistence_id
              """) match {
      case Lexer.Success(tokens) =>
        println("\n" + tokens)
        val result = parseWith(tokens, G.toplevel)
        println(result)
      case Lexer.Failure(error, _) => println("Error " + error)
    }
  }

}


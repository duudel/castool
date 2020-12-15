package castool.rql

final case class InputPos(input: String, line: Int, column: Int)

object Lexer {

  case class Input(input: String, pos: Int) {
    def hasInput(): Boolean = pos < input.length()
    def current(): Char = input.charAt(pos)
    def accept(x: Char): Boolean = hasInput() && current() == x
    def advance(): Input = {
      assert(hasInput())
      copy(pos = pos + 1)
    }
    def substringFrom(startPos: Int): String = input.substring(startPos, pos)
  }

  sealed trait State {
    def isValid: Boolean
    def hasInput: Boolean
  }

  final case class Valid(input: Input, tokens: Vector[Token]) extends State {
    override def isValid: Boolean = true
    override def hasInput: Boolean = input.hasInput()
    def advance(): Valid = copy(input = input.advance())
    def emit(token: Token): Valid = copy(tokens = tokens :+ token)
    def reject: Reject = Reject(this)
  }
  final case class Reject(prev: Valid) extends State {
    override def isValid: Boolean = false
    override def hasInput: Boolean = false
  }

  trait Consumer {
    def accept(state: Valid): State
    def :>(other: Consumer): And = And(this, other)
    def :>(c: Char): And = And(this, AcceptChar(c))
    def :>(s: String): And = And(this, AcceptString(s))
    def :|(other: Consumer): Or = Or(this, other)
    def :|(c: Char): Or = Or(this, AcceptChar(c))
    def :|(s: String): Or = Or(this, AcceptString(s))
    def *(): Consumer = RepeatWhile(this)
    def ![T <: Token](fn: (String, Int) => T): Consumer = Emit(this, fn)
    def ![T <: Token](fn: Int => T): Consumer = Emit(this, (_, pos) => fn(pos))
  }

  case class And(a: Consumer, b: Consumer) extends Consumer {
    def accept(state: Valid): State = a.accept(state) match {
      case next: Valid => b.accept(next)
      case r: Reject => r
    }
  }

  case class Or(a: Consumer, b: Consumer) extends Consumer {
    def accept(state: Valid): State = a.accept(state) match {
      case next: Valid => next
      case r: Reject => b.accept(state)
    }
  }

  case class AcceptChar(c: Char) extends Consumer {
    def accept(state: Valid): State = if (state.input.accept(c)) {
      state.advance()
    } else {
      state.reject
    }
  }

  case class AcceptAny(c: Iterable[Char]) extends Consumer {
    def accept(state: Valid): State = {
      //println("Accept any " + c)
      var it = c.iterator
      while (it.hasNext) {
        if (state.input.accept(it.next())) {
          return state.advance()
        }
      }
      state.reject
    }
  }

  object AcceptAny {
    def apply(c: Char*): AcceptAny = AcceptAny(c)
  }

  case class AcceptAnyBut(c: Iterable[Char]) extends Consumer {
    def accept(state: Valid): State = {
      //println("Accept any " + c)
      var it = c.iterator
      while (it.hasNext) {
        if (state.input.accept(it.next())) {
          return state.reject
        }
      }
      state.advance()
    }
  }

  object AcceptAnyBut {
    def apply(c: Char*): AcceptAnyBut = AcceptAnyBut(c)
  }

  case class AcceptString(str: String) extends Consumer {
    def accept(state: Valid): State = {
      var index = 0
      var s = state
      while (index < str.length && s.input.accept(str.charAt(index))) {
        s = s.advance()
        index += 1
      }
      if (index == str.length()) {
        s
      } else {
        s.reject
      }
    }
  }

  case class Not(c: Consumer) extends Consumer {
    def accept(state: Valid): State = c.accept(state) match {
      case r: Reject => state
      case s: Valid => s.reject
    }
  }

  object Nop extends Consumer {
    def accept(state: Valid): State = state
  }

  case class Invert(c: Consumer) extends Consumer {
    def accept(state: Valid): State = c.accept(state) match {
      case r: Reject => state.advance()
      case s: Valid => s.reject
    }
  }

  case class Emit[T <: Token](c: Consumer, tokenFn: (String, Int) => T) extends Consumer {
    def accept(state: Valid): State = {
      val next = c.accept(state)
      next match {
        case v: Valid => 
          val startPos = state.input.pos
          val value = v.input.substringFrom(startPos)
          v.emit(tokenFn(value, startPos))
        case _ =>
          state.reject
      }
    }
  }

  case class RepeatWhile(c: Consumer) extends Consumer {
    def accept(state: Valid): State = {
      //if (state.input.hasInput()) {
        var s = c.accept(state)
        if (s.isValid) {
          var lastValid = s
          while (s.isValid && s.hasInput) {
            s = c.accept(s.asInstanceOf[Valid])
            if (s.isValid) {
              lastValid = s
            }
          }
          lastValid
        } else {
          s
        }
      //} else {
      //  state.reject
      //}
    }
  }

  def choice(first: Consumer, rest: Consumer*): Consumer = {
    rest.foldLeft(first) { case (acc, c) =>
      acc :| c
    }
  }

  import Token._

  def char(c: Char) = AcceptChar(c)
  def str(s: String) = AcceptString(s)
  def any(c: Char*) = AcceptAny(c)
  def anyBut(c: Char*) = AcceptAnyBut(c)
  val whitespace = AcceptAny(' ', '\r', '\n', '\t').*
  val skipWhitespace = whitespace // :| Nop
  val digit = AcceptAny('0' to '9')
  val exponent = (char('e') :| 'E') :> (char('+') :| '-' :| Nop) :> digit.*
  val decimals = (char('.') :> digit.*)
  val number_ = digit.* :> (decimals :| Nop) :> (exponent :| Nop)
  val alpha = AcceptAny('a' to 'z') :| AcceptAny('A' to 'Z')
  val alphaNumeric = alpha :| digit
  val ident_ = alpha :> (alphaNumeric :| '_').*
  def keyword(k: String) = str(k) :> Not(ident_)

  val singleLineComment = str("//") :> anyBut('\n').*
  val multiLineComment = str("/*") :> (anyBut('*') :| (char('*') :> anyBut('/'))).* :| str("*/")

  val ident = ident_.!(Ident)

  val numberLit = number_.!((s: String, pos: Int) => NumberLit(s.toDouble, pos))

  val stringEscapeSeq = char('\\') :> any('r', 'n', 't', '"', '\'', '\\')
  val stringLitDq = char('"') :> (anyBut('\\', '"') :| stringEscapeSeq).* :> char('"')
  val stringLitSq = char('\'') :> (anyBut('\\', '\'') :| stringEscapeSeq).* :> char('\'')
  val stringLit = (stringLitDq :| stringLitSq).!(StringLit)

  val nullLit = keyword("null").!(NullLit)
  val trueLit = keyword("true").!(TrueLit)
  val falseLit = keyword("false").!(FalseLit)

  val bar = char('|').!(Bar)
  val lparen = char('(').!(LParen)
  val rparen = char(')').!(RParen)
  val comma = char(',').!(Comma)

  val opNot = char('!').!(OpNot)
  val opPlus = char('+').!(OpPlus)
  val opMinus = char('-').!(OpMinus)
  val opMultiply = char('*').!(OpMultiply)
  val opDivide = char('/').!(OpDivide)
  val opAssign = char('=') ! OpAssign
  val opEqual = str("==") ! OpEqual
  val opNotEqual = str("!=") ! OpNotEqual
  val opLess = char('<') ! OpLess
  val opLessEq = str("<=") ! OpLessEq
  val opGreater = char('>') ! OpGreater
  val opGreaterEq = str(">=") ! OpGreaterEq
  val opContains = keyword("contains") ! (OpContains)
  val opNotContains = keyword("!contains") ! (OpNotContains)
  val opAnd = keyword("and") ! (OpAnd)
  val opOr = keyword("or") ! (OpOr)

  val op = choice(
    opPlus, opMinus,
    opMultiply, opDivide,
    opEqual, opNotEqual,
    opAssign,
    opLessEq, opLess,
    opGreaterEq, opGreater,
    opContains, opNotContains,
    opNot,
    opAnd, opOr,
  )

  val tokens = choice(
    singleLineComment, multiLineComment, skipWhitespace, op,
    ident, numberLit, stringLit, bar, lparen, rparen,
  ).*

  sealed trait Result
  final case class Success(tokens: Iterable[Token]) extends Result
  final case class Failure(error: String, pos: Int) extends Result

  def valid(input: String) = Valid(Input(input, pos = 0), Vector.empty)

  def lex(input: String): Result = {
    val state: Valid = valid(input)
    lexState(state) match {
      case Valid(_, tokens) => Success(tokens)
      case Reject(prev) => Failure("Invalid token", prev.input.pos)
    }
  }

  def lexState(state: Valid): State = {
    tokens.accept(state)
  }

  def main() {
    lex("""
      Rows | where persistence_id < 20 and kala <= 12 or ser_is !contains "Event"
      | extend json=parse_json(event) == "<parsed>" // here we are parsing it!
      | /* order by persistence_id */
      """)

    println(numberLit.accept(valid("20.201211e+2")))
  }

}


package castool.rql

//import zio._
import zio.test._
import zio.test.TestAspect._
import zio.test.Assertion._
import zio.test.environment._

object ParserSpec {
  import Parser._
  import Name.NameInterpolator

  def parseTest(s: String)(testImpl: Ast => TestResult): TestResult = {
    Lexer.lex(s) match {
      case Lexer.Success(tokens) =>
        parse(tokens) match {
          case Success(ast) =>
            testImpl(ast)
          case Failure(errors) =>
            assert(errors)(isEmpty)
        }
      case Lexer.Failure(error, loc) =>
        val pos = loc.atSourceText(s)
        assert(pos + ": " + error)(isEmptyString)
    }
  }

  val tests = suite("ParserSpec")(
    test("parse only table query") {
      parseTest("TableName") { result =>
        val tab = Ast.Table(n"TableName", Token.Ident("TableName", 0))
        assert(result)(equalTo(tab))
      }
    } @@ ignore,
    test("parse simple query") {
      parseTest("TableName | where column1") { result =>
        val tab = Ast.Table(n"TableName", Token.Ident("TableName", 0))
        val where = Ast.Where(Ast.Column(n"column1", 18), 12)
        val expected = Ast.Cont(tab, where)
        assert(result)(equalTo(expected))
      }
    },
    test("parse where, extend, and summarize") {
      parseTest("""
      Rows
      | where persistence_id contains "kala"
      | extend json=as_json(event)
      | summarize cnt=count() by persistence_id""") { result =>
        val tab = Ast.Table(n"Rows", Token.Ident("Rows", 7))
        val where = Ast.Where(
          Ast.BinaryExpr(
            BinaryOp.Contains,
            Ast.Column(n"persistence_id", 26),
            Ast.StringLit(Token.StringLit("kala", 50)),
            41),
          20
        )
        val whereCont = Ast.Cont(tab, where)
        val asJsonCall = Ast.FunctionCall(n"as_json", args = Seq(Ast.Column(n"event", 85)), 77)
        val extend = Ast.Extend(
          NameAndToken(n"json", Token.Ident("json", 72)),
          asJsonCall,
          65
        )
        val extendCont = Ast.Cont(whereCont, extend)
        val countCall = Ast.FunctionCall(n"count", args = Seq(), 114)
        val summarize = Ast.Summarize(
          Seq(Ast.Aggregation(NameAndToken(n"cnt", Token.Ident("cnt", 110)), countCall)),
          Seq(NameAndToken(n"persistence_id", Token.Ident("persistence_id", 125))),
          100
        )
        val expected = Ast.Cont(extendCont, summarize)
        assert(result)(equalTo(expected))
      }
    },
    test("parse complex query") {
      parseTest("""
        Source
        | where column1 > column2 and event contains "this and that"
        | order by column2 desc
        | project column1, event
        | extend new_column = func(a, column2) + 20.2
        """) { result =>
          assert(result.isInstanceOf[Ast.Cont])(equalTo(true))
        }
    }
  )
}

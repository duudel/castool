package castool.rql

//import zio._
import zio.test._
import zio.test.Assertion._
import zio.test.environment._

object ParserSpec {
  import Parser._

  def parseTest(s: String)(testImpl: Ast => TestResult): TestResult = {
    Lexer.lex(s) match {
      case Lexer.Success(tokens) =>
        parse(tokens) match {
          case Success(ast) =>
            testImpl(ast)
          case Failure(error, _) =>
            assert(error)(equalTo(""))
        }
      case Lexer.Failure(error, _) =>
        assert(error)(equalTo(""))
    }
  }

  val tests = suite("ParserSpec")(
    test("parse simple query") {
      parseTest("TableName | where column1") { result =>
        val tab = Ast.Table(Name.fromStringUnsafe("TableName"), Token.Ident("TableName", 0))
        val where = Ast.Where(Ast.Column(Name.fromStringUnsafe("column1"), 18), 12)
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
        object names {
          val Rows = Name.fromStringUnsafe("Rows")
          val persistence_id = Name.fromStringUnsafe("persistence_id")
          val json = Name.fromStringUnsafe("json")
          val as_json = Name.fromStringUnsafe("as_json")
          val event = Name.fromStringUnsafe("event")
          val cnt = Name.fromStringUnsafe("cnt")
          val count = Name.fromStringUnsafe("count")
        }
        val tab = Ast.Table(names.Rows, Token.Ident("Rows", 7))
        val where = Ast.Where(
          Ast.BinaryExpr(
            BinaryOp.Contains,
            Ast.Column(names.persistence_id, 26),
            Ast.StringLit(Token.StringLit("\"kala\"", 50)),
            41),
          20
        )
        val whereCont = Ast.Cont(tab, where)
        val asJsonCall = Ast.FunctionCall(names.as_json, args = Seq(Ast.Column(names.event, 85)), 77)
        val extend = Ast.Extend(
          NameAndToken(Name.fromStringUnsafe("json"), Token.Ident("json", 72)),
          asJsonCall,
          65
        )
        val extendCont = Ast.Cont(whereCont, extend)
        val countCall = Ast.FunctionCall(names.count, args = Seq(), 114)
        val summarize = Ast.Summarize(
          Seq(Ast.Aggregation(NameAndToken(names.cnt, Token.Ident("cnt", 110)), countCall)),
          Seq(NameAndToken(names.persistence_id, Token.Ident("persistence_id", 125))),
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

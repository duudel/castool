package castool.rql

//import zio._
import zio.test._
import zio.test.Assertion._
import zio.test.environment._

object SemCheckSpec {
  val tables: Map[Name, SourceDef] = Map(
    Name.fromStringUnsafe("TableName") -> SourceDef(Seq(
      Name.fromStringUnsafe("column1") -> ValueType.Num
    ))
  )

  val env = new SemCheck.Env {
    def tableDef(name: Name): SemCheck.Result[SourceDef] = {
      tables.get(name).map(SemCheck.Success(_)).getOrElse(SemCheck.Failure(s"No such table as '${name.n}'"))
    }
    def functionDef(name: Name): SemCheck.Result[FunctionDef[Value]] = SemCheck.Failure(s"No such function as '${name.n}'")
    def aggregationDef(name: Name): SemCheck.Result[AggregationDef[Value]] = SemCheck.Failure(s"No such aggregation function as '${name.n}'")
  }

  def semCheckTest(s: String)(testImpl: SemCheck.Result[_] => TestResult): TestResult = {
    Lexer.lex(s) match {
      case Lexer.Success(tokens) =>
        Parser.parse(tokens) match {
          case Parser.Success(ast) =>
            val result = SemCheck.check(ast, env)
            testImpl(result)
          case Parser.Failure(error, _) =>
            assert(error)(equalTo(""))
        }
      case Lexer.Failure(error, _) =>
        assert(error)(equalTo(""))
    }
  }

  val tests = suite("SemCheckSpec")(
    test("semantic check fails with non boolean where expression") {
      semCheckTest("TableName | where column1") { result =>
        assert(result.isFailure)(equalTo(true))
      }
    },
    test("semantic check succeeds with boolean where expression") {
      semCheckTest("TableName | where column1 > 10") { result =>
        println(result)
        assert(result.isSuccess)(equalTo(true))
      }
    }
  )
}


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
      zio.ZIO.fromOption(tables.get(name))
        .mapError(_ => SemCheck.Error(s"No such table as ${name.n}", Location(0)))
    }
    def functionDef(name: Name): SemCheck.Result[FunctionDef[Value]] =
      zio.ZIO.fail(SemCheck.Error(s"No such function as '${name.n}'", Location(0)))
    def aggregationDef(name: Name): SemCheck.Result[AggregationDef[Value]] =
      zio.ZIO.fail(SemCheck.Error(s"No such aggregation function as '${name.n}'", Location(0)))
  }

  def semCheckTest(s: String): zio.IO[SemCheck.Error, Checked] = {
    val result = Lexer.lex(s) match {
      case Lexer.Success(tokens) =>
        Parser.parse(tokens) match {
          case Parser.Success(ast) =>
            SemCheck.check(ast)
          case Parser.Failure(error, loc) =>
            zio.ZIO.fail(SemCheck.Error(error, loc))
        }
      case Lexer.Failure(error, loc) =>
        zio.ZIO.fail(SemCheck.Error(error, loc))
    }
    result.provide(env)
  }

  val tests = suite("SemCheckSpec")(
    testM("semantic check fails with non boolean where expression") {
      val result = semCheckTest("TableName | where column1")
      assertM(result)(anything)
    } @@ TestAspect.failing,
    testM("semantic check succeeds with boolean where expression") {
      val result = semCheckTest("TableName | where column1 > 10")
      assertM(result)(anything)
    }
  )
}


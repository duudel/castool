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
    def tableDef(name: Name): Option[SourceDef] = tables.get(name)
    def functionDef(name: Name): Option[FunctionDef[Value]] = None
    def aggregationDef(name: Name): Option[AggregationDef[Value]] = None
  }

  def semCheckTest(s: String): zio.IO[SemCheck.Error, Checked] = {
    val result = Lexer.lex(s) match {
      case Lexer.Success(tokens) =>
        Parser.parse(tokens) match {
          case Parser.Success(ast) =>
            SemCheck.check(ast)
          case Parser.Failure(errors) =>
            zio.ZIO.fail(SemCheck.Error(errors.head.message, errors.head.loc))
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


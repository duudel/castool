package castool.rql

import zio.IO
import zio.ZLayer
import zio.stream._
import zio.test._
import zio.test.Assertion._
import zio.test.environment._

import Name.NameInterpolator

object CompilerSpec {
  val testEnv = new Execution.Env {
    def table(name: Name): Option[Execution.Stream] =
      if (name == n"TestTable")
        Some(ZStream(
          InputRow(n"test_column" -> Str("Hello there!"), n"flag" -> Bool(true)),
          InputRow(n"test_column" -> Str("Row number 2!"), n"flag" -> Bool(false)),
        ))
      else None
  }

  val testCompilationEnv = new Compiler.Env {
    def tableDef(name: Name): SemCheck.Result[SourceDef] = {
      if (name == n"TestTable")
        SemCheck.Success(SourceDef(n"test_column" -> ValueType.Str, n"flag" -> ValueType.Bool))
      else SemCheck.Failure(s"No such table as '${name.n}'")
    }
    def functionDef(name: Name): SemCheck.Result[FunctionDef[Value]] =
      if (name == n"not")
        SemCheck.Success(FunctionDef(Eval((b: Bool) => Bool(!b.v)), Map("b" -> ValueType.Bool), ValueType.Bool))
      else SemCheck.Failure(s"No such function as '${name.n}'")
    def aggregationDef(name: Name): SemCheck.Result[AggregationDef[Value]] = ???
  }

  def testCompilation(s: String): IO[Compiler.Error, Compiled.Source] = {
    val result = Compiler.compileQuery(s)
    result.provide(testCompilationEnv)
  }

  val isSuccess = Assertion.assertion("compilation result")()((a: Any) => true)

  val tests = suite("CompilerSpec")(
    testM("compile source table only query") {
      for {
        result <- testCompilation("TestTable")
      } yield assert(result.isInstanceOf[Compiled.Source])(equalTo(true))
    },
    testM("compile simple query") {
      val e = for {
        result <- testCompilation("TestTable | where test_column contains \"Hello\" or not(flag)")
        _ <- Execution.run(result).foreach { input =>
          zio.console.putStrLn(input.values.toString())
        }
      } yield assert(result)(isSuccess)
      type Layer = zio.console.Console with Execution.EnvService
      val compiledEnv: ZLayer[Any, Any, Execution.EnvService] = Execution.live(testEnv)
      val layer0: ZLayer[Any, Any, Layer] = zio.console.Console.live ++ compiledEnv
      e.provideLayer(layer0)
    },
    testM("compile simple order by query") {
      val e = for {
        result <- testCompilation("TestTable | where test_column contains \"Hello\" or not(flag) | order by test_column desc")
        _ <- Execution.run(result).foreach { input =>
          zio.console.putStrLn(input.values.toString())
        }
      } yield assert(result)(isSuccess)
      type Layer = zio.console.Console with Execution.EnvService
      val compiledEnv: ZLayer[Any, Any, Execution.EnvService] = Execution.live(testEnv)
      val layer0: ZLayer[Any, Any, Layer] = zio.console.Console.live ++ compiledEnv
      e.provideLayer(layer0)
    }
  )
}


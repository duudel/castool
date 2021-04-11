package castool.rql

import zio.IO
import zio.ZLayer
import zio.stream._
import zio.test._
import zio.test.Assertion._
import zio.test.environment._

import Name.NameInterpolator

object CompilerSpec {
  val testEnv = new Compiled.Env {
    def table(name: Name): Option[Compiled.Stream] =
      if (name == n"TestTable")
        Some(ZStream(
          InputRow(n"test_column" -> Str("Hello there!"), n"flag" -> Bool(true)),
          InputRow(n"test_column" -> Str("Row number 2!"), n"flag" -> Bool(false)),
        ))
      else None
    def tableDef(name: Name): Option[SourceDef] = {
      if (name == n"TestTable")
        Some(SourceDef(n"test_column" -> ValueType.Str, n"flag" -> ValueType.Bool))
      else None
    }
    def function(name: Name): Option[FunctionDef[_]] =
      if (name == n"not")
        Some(FunctionDef(Eval((b: Bool) => Bool(!b.v)), Map("b" -> ValueType.Bool), ValueType.Bool))
      else None
    def aggregation(name: Name): Option[FunctionDef[_]] = ???
  }

  def testCompilation(s: String): IO[Compiler.Error, Compiled.Source] = {
    val result = Compiler.compileQuery(s)
    result.provide(testEnv)
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
      type Layer = zio.console.Console with Compiled.EnvService
      val compiledEnv: ZLayer[Any, Any, Compiled.EnvService] = Compiled.live(testEnv)
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
      type Layer = zio.console.Console with Compiled.EnvService
      val compiledEnv: ZLayer[Any, Any, Compiled.EnvService] = Compiled.live(testEnv)
      val layer0: ZLayer[Any, Any, Layer] = zio.console.Console.live ++ compiledEnv
      e.provideLayer(layer0)
    }
  )
}


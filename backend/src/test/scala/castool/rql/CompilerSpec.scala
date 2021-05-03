package castool.rql

import zio.IO
import zio.ZLayer
import zio.stream._
import zio.test._
import zio.test.Assertion._
import zio.test.environment._

import Name.NameInterpolator

object CompilerSpec {
  def testEnv = new Execution.Env {
    def table(name: Name): Option[Execution.Stream] =
      if (name == n"TestTable")
        Some(ZStream(
          InputRow(n"test_column" -> Str("Hello there!"),   n"flag" -> Bool(true)),
          InputRow(n"test_column" -> Str("Row number 2!"),  n"flag" -> Bool(false)),
          InputRow(n"test_column" -> Str("test"),           n"flag" -> Bool(false)),
          InputRow(n"test_column" -> Str("stuff"),          n"flag" -> Bool(false)),
        ))
      else None
  }

  val testCompilationEnv = {
    import ResolveValueType._
    val functions = new Functions()
      .addFunction(n"not", Seq("b" -> ValueType.Bool), Eval((b: Bool) => Bool(!b.v)))
      .addAggregation(n"count", Seq("x" -> ValueType.Num), Num(0), Eval(() => Num(1)), (_: Num, n) => n)
    new Compiler.Env {
      def tableDef(name: Name): Option[SourceDef] = {
        if (name == n"TestTable") Some(SourceDef(n"test_column" -> ValueType.Str, n"flag" -> ValueType.Bool))
        else None
      }
      def functionDef(name: Name, argTypes: Seq[ValueType]): Option[FunctionDef[Value]] = functions.functionDef(name, argTypes)
      def aggregationDef(name: Name): Option[AggregationDef[Value]] = functions.aggregationDef(name)
    }
  }

  type ExecLayer = zio.console.Console with Execution.EnvService
  val execEnvLayer: ZLayer[Any, Any, Execution.EnvService] = Execution.live(testEnv)
  val layer0: ZLayer[Any, Any, ExecLayer] = zio.console.Console.live ++ execEnvLayer

  def testCompilation(s: String): IO[Compiler.Errors, Compiled.Source] = {
    val result = Compiler.compileQuery(s)
    result.provide(testCompilationEnv)
  }

  def testQuery(desc: String)(body: => zio.ZIO[ExecLayer, Any, TestResult]): ZSpec[Any, Any] = {
    testM(desc)(body.provideLayer(layer0))
  }

  val isSuccess = Assertion.assertion("compilation result")()((a: Any) => true)

  val tests = suite("CompilerSpec")(
    testQuery("compile source table only query") {
      for {
        compiled <- testCompilation("TestTable")
        result <- Execution.run(compiled).runCollect
        rows = result.toArray
      } yield assert(rows)(equalTo(Array(
        InputRow(n"test_column" -> Str("Hello there!"),   n"flag" -> Bool(true)),
        InputRow(n"test_column" -> Str("Row number 2!"),  n"flag" -> Bool(false)),
        InputRow(n"test_column" -> Str("test"),           n"flag" -> Bool(false)),
        InputRow(n"test_column" -> Str("stuff"),          n"flag" -> Bool(false)),
      )))
    },
    testQuery("compile simple query") {
      for {
        compiled <- testCompilation("TestTable | where test_column contains \"Hello\" and !not(flag)")
        result <- Execution.run(compiled).runCollect
        rows = result.toArray
      } yield assert(rows)(equalTo(Array(
        InputRow(n"test_column" -> Str("Hello there!"),   n"flag" -> Bool(true)),
      )))
    },
    testQuery("compile simple order by query") {
      for {
        compiled <- testCompilation("TestTable | where test_column contains \"Hello\" or not(flag) | order by test_column desc")
        _ <- Execution.run(compiled).foreach { input =>
          zio.console.putStrLn(input.values.toString())
        }
        result <- Execution.run(compiled).fold(Vector.empty[InputRow]) { case (acc, row) => acc :+ row }
      } yield assert(result)(equalTo(
        Vector(
          InputRow(n"test_column" -> Str("test"),           n"flag" -> Bool(false)),
          InputRow(n"test_column" -> Str("stuff"),          n"flag" -> Bool(false)),
          InputRow(n"test_column" -> Str("Row number 2!"),  n"flag" -> Bool(false)),
          InputRow(n"test_column" -> Str("Hello there!"),   n"flag" -> Bool(true)),
        )
      ))
    },
    testQuery("compile simple aggregation query") {
      for {
        compiled <- testCompilation("TestTable | summarize cnt=count()")
        result <- Execution.run(compiled).fold(Vector.empty[InputRow]){ case (acc, row) => acc :+ row }
      } yield {
        val expected = InputRow(n"cnt" -> Num(4))
        assert(result)(hasAt(0)(equalTo(expected)))
      }
    },
    testQuery("compile aggregation query with group by") {
      for {
        compiled <- testCompilation("TestTable | summarize cnt=count() by flag | order by flag asc")
        result <- Execution.run(compiled).fold(Vector.empty[InputRow]){ case (acc, row) => acc :+ row }
      } yield {
        val expected = Vector(
          InputRow(n"flag" -> Bool(false),  n"cnt" -> Num(3)),
          InputRow(n"flag" -> Bool(true),   n"cnt" -> Num(1)),
        )
        assert(result)(equalTo(expected))
      }
    }
  )
}


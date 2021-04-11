package castool.rql

import zio.test._
import zio.test.Assertion._
import zio.test.environment._

object AllSuites extends DefaultRunnableSpec {
  def spec = suite("All rql tests")(
    ParserSpec.tests,
    SemCheckSpec.tests,
    CompilerSpec.tests,
  )
}


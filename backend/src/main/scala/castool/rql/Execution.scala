package castool.rql

object Execution {
  def run(q: Compiled.Source): Compiled.Stream = {
    q.stream
  }
}

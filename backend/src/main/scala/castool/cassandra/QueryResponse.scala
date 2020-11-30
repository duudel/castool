package castool.cassandra

import zio._
import zio.stream._

case class QueryResponse(
  columnDefs: Seq[ColumnDefinition],
  executionInfo: ExecutionInfo,
  rowsStream: ZStream[Any, Throwable, ResultRow]
)


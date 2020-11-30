package castool.cassandra

import com.datastax.oss.driver.api.core.cql

import scala.collection.JavaConverters._

case class ExecutionInfo(
  responseSizeBytes: Int,
  compressedResponseSizeBytes: Int,
  coordinator: Node,
  speculativeExecCount: Int,
  successfulExecIndex: Int,
  warnings: Seq[String]
)

object ExecutionInfo {
  def fromDriver(info: cql.ExecutionInfo): ExecutionInfo = {
    ExecutionInfo(
      responseSizeBytes = info.getResponseSizeInBytes(),
      compressedResponseSizeBytes = info.getCompressedResponseSizeInBytes(),
      coordinator = Node.fromDriverNode(info.getCoordinator()),
      speculativeExecCount = info.getSpeculativeExecutionCount(),
      successfulExecIndex = info.getSuccessfulExecutionIndex(),
      warnings = info.getWarnings().asScala.toSeq
    )
  }
}

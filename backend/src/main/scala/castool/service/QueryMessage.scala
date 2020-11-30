package castool.service

import io.circe._
import io.circe.generic.auto._
import io.circe.generic.semiauto._

import castool.cassandra.{ColumnDefinition, ColumnValue, ExecutionInfo, ResultRow}

sealed trait QueryMessage;

case class QueryMessageSuccess(columns: Seq[ColumnDefinition], executionInfo: ExecutionInfo) extends QueryMessage 
case class QueryMessageError(error: String) extends QueryMessage
case class QueryMessageRows(rows: Seq[ResultRow]) extends QueryMessage
case object QueryMessageFinished extends QueryMessage

object QueryMessage {
  implicit val decoder: Decoder[QueryMessage] = deriveDecoder
  implicit val encoder: Encoder[QueryMessage] = deriveEncoder
}


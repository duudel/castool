package castool.service

import io.circe._
import io.circe.generic.auto._
import io.circe.generic.semiauto._

import castool.cassandra.{ColumnDefinition, ColumnValue, ResultRow}

sealed trait QueryMessage;

case class QueryMessageSuccess(columns: Seq[ColumnDefinition]) extends QueryMessage 
case class QueryMessageError(error: String) extends QueryMessage
case class QueryMessageRows(rows: Seq[ResultRow]) extends QueryMessage
case object QueryMessageFinished extends QueryMessage

object QueryMessage {
  implicit val decoder: Decoder[QueryMessage] = deriveDecoder
  implicit val encoder: Encoder[QueryMessage] = deriveEncoder
}

/*
object Test {
  def printJson(a: QueryMessage): Unit = {
    println(a.asJson)
  }

  def doIt() {
    printJson(QueryMessageSuccess)
    printJson(QueryMessageError("Syntax was bad"))
    printJson(QueryMessageRows(Seq(ResultRow(10L, Seq(castool.cassandra.Text("Text value"))))))
  }
}
*/



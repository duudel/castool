package castool.cassandra

import cats.syntax.functor._
import io.circe.{Decoder, Encoder}
import io.circe.generic.auto._

case class ResultRow(index: Long, columnValues: Seq[ColumnValue])

object ResultRow {
  implicit val decoder: Decoder[ResultRow] = Decoder[ResultRow].widen
}



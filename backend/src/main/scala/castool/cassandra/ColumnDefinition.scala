package castool.cassandra

import io.circe.{Decoder, Encoder}
import io.circe._
import io.circe.generic.semiauto._

import com.datastax.oss.driver

case class ColumnDefinition(
  name: String,
  dataType: ColumnValue.DataType
)

object ColumnDefinition {
  //implicit val codec = deriveCodec[ColumnDefinition]
  implicit val encoder: Encoder[ColumnDefinition] = deriveEncoder

  def fromDriverDef(defn: driver.api.core.cql.ColumnDefinition): ColumnDefinition = ColumnDefinition(
    name = defn.getName().asCql(true),
    dataType = ColumnValue.DataType.fromCas(defn.getType())
  )
}

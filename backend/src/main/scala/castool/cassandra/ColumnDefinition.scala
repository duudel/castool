package castool.cassandra

import com.datastax.oss.driver

case class ColumnDefinition(
  name: String,
  dataType: ColumnValue.DataType
)

object ColumnDefinition {
  def fromDriverDef(defn: driver.api.core.cql.ColumnDefinition): ColumnDefinition = ColumnDefinition(
    name = defn.getName().asCql(true),
    dataType = ColumnValue.DataType.fromCas(defn.getType())
  )
}

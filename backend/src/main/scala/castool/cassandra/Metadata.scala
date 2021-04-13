package castool.cassandra

import io.circe.{Decoder, Encoder}
import io.circe._
//import io.circe.generic.semiauto.deriveDecoder
import io.circe.generic.semiauto._

import com.datastax.oss.driver

import scala.collection.JavaConverters._
import java.util.UUID

object util {
  def fromOptional[A](opt: java.util.Optional[A]): Option[A] = if (opt.isPresent()) Some(opt.get) else None
}

object ClusteringOrder extends Enumeration {
  val ASC = Value
  val DESC = Value

  implicit val decoder: Decoder[Value] = Decoder.decodeString.emap {
    case "ASC" => Right(ASC)
    case "DESC" => Right(DESC)
    case s => Left(s"Unrecognised clustering order $s")
  }

  implicit val encoder: Encoder[Value] = Encoder.encodeString.contramap(_.toString)
}

case class Node(
  hostId: UUID,
  cassandraVersion: String,
  datacenter: String,
  endpoint: String,
  rack: Option[String],
  schemaVersion: Option[UUID]
)

object Node {
  implicit val decoder: Decoder[Node] = deriveDecoder

  def fromDriverNode(node: driver.api.core.metadata.Node): Node = Node(
    hostId = node.getHostId(),
    cassandraVersion = node.getCassandraVersion().toString,
    datacenter = node.getDatacenter(),
    endpoint = node.getEndPoint().resolve().toString(),
    rack = Option(node.getRack()),
    schemaVersion = Option(node.getSchemaVersion())
  )
}

case class Keyspace(name: String, replication: Map[String, String], tables: Map[String, Table])

object Keyspace {
  implicit val decoder: Decoder[Keyspace] = deriveDecoder

  def fromDriverKeyspace(k: driver.api.core.metadata.schema.KeyspaceMetadata): Keyspace = Keyspace(
    name = k.getName().asCql(true),
    replication = k.getReplication().asScala.toMap[String, String],
    tables = k.getTables().asScala.map { case (name, table) => name.asCql(true) -> Table.fromDriverTable(table) }.toMap[String, Table]
  )
}

case class Table(
  id: Option[UUID],
  name: String,
  columnDefs: Map[String, ColumnDefinition],
  partitionKey: Seq[String],
  clusteringColumns: Map[String, ClusteringOrder.Value]
)

object Table {
  implicit val decoder: Decoder[Table] = deriveCodec

  def fromDriverTable(t: driver.api.core.metadata.schema.TableMetadata): Table = {
    val columnDefs = t.getColumns().asScala.map { case (name, column) =>
      val nameString = name.asCql(true)
      nameString -> ColumnDefinition(name = nameString, ColumnValue.DataType.fromCas(column.getType()))
    }
    val partitionKey = t.getPartitionKey().asScala.map(_.getName().asCql(true))
    val clusteringColumns = t.getClusteringColumns().asScala.map {
      case (cm, driver.api.core.metadata.schema.ClusteringOrder.ASC) =>
        cm.getName().asCql(true) -> ClusteringOrder.ASC
      case (cm, driver.api.core.metadata.schema.ClusteringOrder.DESC) =>
        cm.getName().asCql(true) -> ClusteringOrder.DESC
    }
    Table(
      id = util.fromOptional(t.getId()),
      name = t.getName().asCql(true),
      columnDefs = columnDefs.toMap[String, ColumnDefinition],
      partitionKey = partitionKey.toSeq,
      clusteringColumns = clusteringColumns.toMap[String, ClusteringOrder.Value],
    )
  }
}

case class Metadata(clusterName: Option[String], nodes: Seq[Node], keyspaces: Seq[Keyspace])

object Metadata {
  implicit val decoder: Decoder[Metadata] = deriveDecoder

  def fromDriverMetadata(metadata: driver.api.core.metadata.Metadata): Metadata = {
    val keyspaces = metadata.getKeyspaces().asScala.map { case (name, keyspace) => Keyspace.fromDriverKeyspace(keyspace) }
    Metadata(
      clusterName = util.fromOptional(metadata.getClusterName()),
      nodes = metadata.getNodes().asScala.values.map(Node.fromDriverNode).toSeq,
      keyspaces = keyspaces.toVector,
    )
  }
}


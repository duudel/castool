package castool.cassandra

import io.circe.{Decoder, Encoder}
import io.circe._
import io.circe.generic.semiauto._

import com.datastax.oss.driver

import scala.jdk.CollectionConverters._
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
  implicit val encoder: Encoder[Node] = deriveEncoder

  def fromDriverNode(node: driver.api.core.metadata.Node): Node = Node(
    hostId = node.getHostId(),
    cassandraVersion = node.getCassandraVersion().toString,
    datacenter = node.getDatacenter(),
    endpoint = node.getEndPoint().resolve().toString(),
    rack = Option(node.getRack()),
    schemaVersion = Option(node.getSchemaVersion())
  )
}

case class Keyspace(name: String, replication: Map[String, String], tables: Seq[Table])

object Keyspace {
  implicit val encoder: Encoder[Keyspace] = deriveEncoder

  def fromDriverKeyspace(k: driver.api.core.metadata.schema.KeyspaceMetadata): Keyspace = Keyspace(
    name = k.getName().asCql(true),
    replication = k.getReplication().asScala.toMap[String, String],
    tables = k.getTables().asScala.toVector.map { case (name, table) => Table.fromDriverTable(table) }
  )
}

case class Table(
  id: Option[UUID],
  name: String,
  columnDefs: Seq[ColumnDefinition],
  partitionKey: Seq[String],
  clusteringColumns: Seq[(String, ClusteringOrder.Value)]
)

object Table {
  implicit val encoder: Encoder[Table] = deriveEncoder

  def fromDriverTable(t: driver.api.core.metadata.schema.TableMetadata): Table = {
    val columnDefs = t.getColumns().asScala.map { case (name, column) =>
      val nameString = name.asCql(true)
      ColumnDefinition(name = nameString, ColumnValue.DataType.fromCas(column.getType()))
    }
    val partitionKey = t.getPartitionKey().asScala.map(_.getName().asCql(true))
    val clusteringColumns = t.getClusteringColumns().asScala.toSeq.map {
      case (cm, driver.api.core.metadata.schema.ClusteringOrder.ASC) =>
        cm.getName().asCql(true) -> ClusteringOrder.ASC
      case (cm, driver.api.core.metadata.schema.ClusteringOrder.DESC) =>
        cm.getName().asCql(true) -> ClusteringOrder.DESC
    }
    Table(
      id = util.fromOptional(t.getId()),
      name = t.getName().asCql(true),
      columnDefs = columnDefs.toSeq,
      partitionKey = partitionKey.toSeq,
      clusteringColumns = clusteringColumns
    )
  }
}

case class Metadata(clusterName: Option[String], nodes: Seq[Node], keyspaces: Seq[Keyspace])

object Metadata {
  implicit val encoder: Encoder[Metadata] = deriveEncoder

  def fromDriverMetadata(metadata: driver.api.core.metadata.Metadata): Metadata = {
    val keyspaces = metadata.getKeyspaces().asScala.map { case (name, keyspace) => Keyspace.fromDriverKeyspace(keyspace) }
    Metadata(
      clusterName = util.fromOptional(metadata.getClusterName()),
      nodes = metadata.getNodes().asScala.values.map(Node.fromDriverNode).toSeq,
      keyspaces = keyspaces.toVector,
    )
  }
}


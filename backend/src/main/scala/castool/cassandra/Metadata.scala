package castool.cassandra

import io.circe.{Decoder, Encoder}
import io.circe._
//import io.circe.generic.semiauto.deriveDecoder
import io.circe.generic.semiauto._

import com.datastax.oss.driver

import scala.collection.JavaConverters._
import java.util.UUID

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

case class Metadata(clusterName: Option[String], nodes: Seq[Node])

object Metadata {
  implicit val decoder: Decoder[Metadata] = deriveDecoder

  def fromOptional[A](opt: java.util.Optional[A]): Option[A] = if (opt.isPresent()) Some(opt.get) else None

  def fromDriverMetadata(metadata: driver.api.core.metadata.Metadata): Metadata = Metadata(
    clusterName = fromOptional(metadata.getClusterName()),
    nodes = metadata.getNodes().asScala.values.map(Node.fromDriverNode).toSeq
  )
}


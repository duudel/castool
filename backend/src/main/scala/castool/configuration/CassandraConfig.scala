package castool.configuration

import pureconfig.ConfigConvert
import pureconfig.generic.semiauto.deriveConvert
import zio._

object CassandraConfig {
  case class Config(host: String, port: Int, localDatacenter: String)
  object Config {
    implicit val convert: ConfigConvert[Config] = deriveConvert
  }

  def fromConfiguration: ZLayer[Configuration, Nothing, CassandraConfig] =
    ZLayer.fromService(_.cassandra)
}


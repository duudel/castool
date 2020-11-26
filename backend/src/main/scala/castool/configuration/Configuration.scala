package castool.configuration

import pureconfig.{ConfigConvert, ConfigSource}
import pureconfig.generic.semiauto.deriveConvert
import zio._

object Configuration {
  case class Config(cassandra: CassandraConfig.Config)
  object Config {
    implicit val convert: ConfigConvert[Config] = deriveConvert
  }

  val live: ZLayer[Any, IllegalStateException, Configuration] = ZLayer.fromEffect {
    ZIO
      .fromEither(ConfigSource.default.load[Config])
      .mapError(failures =>
          new IllegalStateException(s"Error loading configuration: $failures")
      )
  }
}


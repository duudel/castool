package castool

import zio._

package object configuration {
  type Configuration = Has[Configuration.Config]
  type CassandraConfig = Has[CassandraConfig.Config]

  def getCassandraConfig: URIO[CassandraConfig, CassandraConfig.Config] = ZIO.access(_.get)
}


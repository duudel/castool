package castool.cassandra

import java.net.InetSocketAddress

import fs2.Stream
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration._

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql

import castool.configuration._

object CassandraSession {
  type CassandraSession = Has[Service]

  trait Service {
    //def query(q: String): IO[Throwable, Stream[Task, ResultRow]]
    def query(q: String): IO[Throwable, cql.ResultSet]
    def metadata: IO[Throwable, Metadata]
  }

  def layer: ZLayer[CassandraConfig with Clock, Throwable, CassandraSession] = {
    def init(config: CassandraConfig.Config): RIO[Clock, CqlSession] = {
      val task = RIO {
        CqlSession.builder()
          .addContactPoint(new InetSocketAddress(config.host, config.port))
          .withLocalDatacenter(config.localDatacenter)
          .build()
      }
      task.catchSome {
        case ex: com.datastax.oss.driver.api.core.AllNodesFailedException =>
          task.retry(Schedule.fibonacci(1.second).map(d => if (d > 10.seconds) 10.seconds else d))
      }
    }

    ZLayer.fromManaged {
      for {
        config  <- getCassandraConfig.toManaged_
        session <- init(config).toManaged_
      } yield new CassandraSessionImpl(session)
    }
  }

  //def query(q: String): RIO[CassandraSession, Stream[Task, ResultRow]] = ZIO.accessM(_.get.query(q))
  def query(q: String): RIO[CassandraSession, cql.ResultSet] = ZIO.accessM(_.get.query(q))
  def metadata: RIO[CassandraSession, Metadata] = ZIO.accessM(_.get.metadata)
}


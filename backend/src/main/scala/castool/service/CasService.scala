package castool.service

import fs2.concurrent.Queue
import io.circe.{Decoder, Encoder}
import io.circe.Json
import io.circe.generic.auto._
import io.circe.generic.semiauto._
import io.circe.syntax._
import org.http4s._
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl
import org.http4s.implicits._
import org.http4s.server._
import org.http4s.server.blaze.BlazeServerBuilder
import org.http4s.server.middleware.CORS
import zio._
import zio.stream._
import zio.stream.interop.fs2z._
import zio.interop.catz._
//import zio.interop.catz.implicits._
import com.datastax.oss.driver.api.core.cql

import scala.collection.JavaConverters._

import castool.configuration
import castool.cassandra.{CassandraSession, ColumnValue, ResultRow, Metadata}
import com.datastax.oss.driver.api.core.`type`.DataTypes
import org.http4s.server.websocket.WebSocketBuilder
import org.http4s.websocket.WebSocketFrame

class CasService[R <: CasService.AppEnv with zio.console.Console] { //[F[_]: Effect: ContextShift] extends Http4sDsl[F] {
  type STask[A] = RIO[R, A]
  val dsl = Http4sDsl[STask]
  import dsl._

  implicit def circeJsonDecoder[A: Decoder]: EntityDecoder[STask, A] = jsonOf[STask, A]
  implicit def circeJsonEncoder[A: Encoder]: EntityEncoder[STask, A] = jsonEncoderOf[STask, A]

  def routes: HttpApp[STask] = Router[STask](
    "" -> CORS(rootRoutes),
    "/auth" -> CORS(authRoutes),
  ).orNotFound

    private def extractRows[R](result: cql.ResultSet): ZStream[R, Throwable, ResultRow] = {
      def singleRow(index: Long, row: cql.Row): ResultRow = {
        val values = result.getColumnDefinitions().iterator().asScala.zipWithIndex.map {
          case (cd, index) => ColumnValue.fromColumnDefinitionAndRow(cd, row, index)
        }
        ResultRow(index, values.toSeq)
      }
      val stream: ZStream[R, Throwable, ResultRow] = ZStream
        .fromIterator(result.iterator().asScala)//(cats.effect.Concurrent.apply[Task])
        .zipWithIndex.map { case (row, index) => singleRow(index, row) }
      stream
    }

    case class ConvPipe[R, I, O](zpipe: ZStream[R, Throwable, I] => ZStream[R, Throwable, O]) {
      type F[A] = RIO[R, A]
      def conv: fs2.Pipe[F, I, O] = {
        inStream: fs2.Stream[F, I] => 
          val zinStream = inStream.toZStream[R]()
          val zoutStream = zpipe(zinStream)
          zoutStream.toFs2Stream
      }
    }

  def rootRoutes: HttpRoutes[STask] = HttpRoutes.of[STask] {
    case GET -> Root =>
      Ok(10)

    case req @ GET -> Root / "squery" =>
      val builder = WebSocketBuilder[STask]

      val PolicyViolation = 1008

      def handleQuery(query: String): STask[ZStream[R, Throwable, QueryMessage]] = {
        for {
          _ <- zio.console.putStrLn("Query: " + query)
          result <- CassandraSession.query(query)
            .map { queryResult =>
              val rowsStream = extractRows(queryResult)
                .groupedWithin(100, zio.duration.Duration.fromMillis(10))
                .map(rows => QueryMessageRows(rows.iterator.toSeq))
              val columnDefs = queryResult.getColumnDefinitions().asScala.map { casColumnDef =>
                val name = casColumnDef.getName().asCql(true)
                ColumnDefinition(
                  name = name,
                  dataType = ColumnValue.DataType.fromCas(casColumnDef.getType())
                )
              }.toSeq
              ZStream(QueryMessageSuccess(columnDefs)) ++ rowsStream
            }
            .catchSome {
              case ex: com.datastax.oss.driver.api.core.servererrors.InvalidQueryException =>
                ZIO.succeed(ZStream(QueryMessageError(ex.getMessage())))
              case ex: com.datastax.oss.driver.api.core.servererrors.SyntaxError =>
                ZIO.succeed(ZStream(QueryMessageError(ex.getMessage())))
            }
        } yield result
      }

      val result = for {
        queue <- zio.ZQueue.bounded[String](10)
        send = ZStream.fromQueue(queue).mapM(handleQuery).flatten.map(qmsg => WebSocketFrame.Text(qmsg.asJson.toString))
        receive = {
          val recv: ZStream[R, Throwable, WebSocketFrame] => ZStream[R, Throwable, Unit] = _.mapM {
            case WebSocketFrame.Text(query, _) =>
              queue.offer(query).unit
            case WebSocketFrame.Close(_) =>
              queue.shutdown
          }
          recv
        }
        result <- builder.build(send.toFs2Stream, ConvPipe[R, WebSocketFrame, Unit](receive).conv)
      } yield result

      result

    case GET -> Root / "metadata" => 
      CassandraSession.metadata.flatMap(Ok(_))
  }

  def authRoutes: HttpRoutes[STask] = HttpRoutes.of[STask] {
    case _ => BadRequest(10)
  }

}

object CasService {
  def apply[R <: AppEnv](): CasService[R] = {
    // Test.doIt()
    new CasService[R]()
  }

  import castool.configuration._
  import zio.blocking.Blocking
  type Layer0Env = Configuration with Blocking with zio.console.Console
  type Layer1Env = Layer0Env with CassandraConfig with zio.clock.Clock
  type Layer2Env = Layer1Env with CassandraSession with zio.clock.Clock
  type AppEnv = Layer2Env with zio.console.Console

  val layer0: ZLayer[Blocking, Throwable, Layer0Env]  = Blocking.any ++ zio.console.Console.live ++ Configuration.live
  val layer1: ZLayer[Layer0Env, Throwable, Layer1Env] = CassandraConfig.fromConfiguration ++ zio.clock.Clock.live ++ ZLayer.identity
  val layer2: ZLayer[Layer1Env, Throwable, Layer2Env] = CassandraSession.layer ++ zio.clock.Clock.live ++ ZLayer.identity

  val appLayer: ZLayer[ZEnv, Throwable, AppEnv] = layer0 >>> layer1 >>> layer2
}


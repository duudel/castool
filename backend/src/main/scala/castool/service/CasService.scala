package castool.service

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
import org.http4s.server.middleware.CORS
import org.http4s.websocket.WebSocketFrame
import zio._
import zio.stream._
import zio.interop.catz._

import com.datastax.oss.driver.api.core.cql
import com.datastax.oss.driver.api.core.`type`.DataTypes

import scala.jdk.javaapi.CollectionConverters._

import castool.configuration
import castool.cassandra.{CassandraSession, ColumnValue, ResultRow, Metadata, QueryResponse}
import castool.util.{ZioWebSocket, ZPipe}

class CasService[R <: CasService.AppEnv] { //[F[_]: Effect: ContextShift] extends Http4sDsl[F] {
  type STask[A] = RIO[R, A]
  val dsl = Http4sDsl[STask]
  import dsl._

  implicit def circeJsonDecoder[A: Decoder]: EntityDecoder[STask, A] = jsonOf[STask, A]
  implicit def circeJsonEncoder[A: Encoder]: EntityEncoder[STask, A] = jsonEncoderOf[STask, A]

  def routes: HttpApp[STask] = Router[STask](
    "" -> CORS(rootRoutes),
    "/auth" -> CORS(authRoutes),
  ).orNotFound

  val PolicyViolation = 1008
  val paging = 1000

  def rootRoutes: HttpRoutes[STask] = HttpRoutes.of[STask] {
    case GET -> Root =>
      Ok(10)

    case req @ GET -> Root / "rql" =>
      def handleQuery(query: String): STask[ZStream[R, Throwable, RqlMessage]] = {
        val queryResult = for {
          _ <- zio.console.putStrLn("Query: " + query)
          queryResult <- RqlService.query(query)
        } yield {
          val RqlService.QueryResult(columnDefs, rowsStream) = queryResult
          val results = rowsStream
            .groupedWithin(paging, zio.duration.Duration.fromMillis(100))
            .map(rows => RqlMessage.Rows(rows.toVector))
          ZStream(RqlMessage.Success(columnDefs)) ++
            results ++
            ZStream(RqlMessage.Finished)
        }
        queryResult.catchAll {
          serviceErrors: Seq[RqlService.Error] =>
            val errors = serviceErrors.flatMap { error =>
              error.loc match {
                case Some(loc) =>
                  val line = loc.line
                  val column = loc.column
                  import scala.jdk.CollectionConverters._
                  val (lineString, _) = query.foldLeft(("", line)) {
                    case ((result, lines), c) if c == '\n' =>
                      (result, lines - 1)
                    case ((result, lines), c) if lines == 1 =>
                      (result + c, lines)
                    case (acc, lines) => acc
                  }
                  val pointerString = "-".*(column) + "^"
                  Seq(
                    s"$line:$column: " + error.msg,
                    lineString,
                    pointerString
                  )
                case None =>
                  Seq(error.msg)
              }
            }
            val errorMessage = RqlMessage.Error(errors)
            ZIO.succeed(ZStream(errorMessage))
        }
      }

      def makeReceive(queue: Queue[String]): ZPipe[R, Throwable, WebSocketFrame, Unit] = _.mapM {
        case WebSocketFrame.Text(query, _) =>
          queue.offer(query).unit
        case WebSocketFrame.Close(_) =>
          queue.shutdown
      }

      val result = for {
        queue <- zio.ZQueue.bounded[String](10)
        send = ZStream.fromQueue(queue).mapM(handleQuery).flatten.map(qmsg => WebSocketFrame.Text(qmsg.asJson.toString))
        receive = makeReceive(queue)
        result <- ZioWebSocket[R].build(send, receive)
      } yield result

      result

    case req @ GET -> Root / "squery" =>
      def handleQuery(query: String): STask[ZStream[R, Throwable, QueryMessage]] = {
        for {
          _ <- zio.console.putStrLn("Query: " + query)
          result <- CassandraSession.query(query)
            .map { case QueryResponse(columnDefs, execInfo, rowsStream) =>
              val resultsStream = rowsStream
                .groupedWithin(100, zio.duration.Duration.fromMillis(100))
                .map(rows => QueryMessageRows(rows.iterator.toSeq))
              ZStream(QueryMessageSuccess(columnDefs, execInfo)) ++ resultsStream ++ ZStream(QueryMessageFinished)
            }
            .catchSome {
              case ex: com.datastax.oss.driver.api.core.servererrors.InvalidQueryException =>
                ZIO.succeed(ZStream(QueryMessageError(ex.getMessage())))
              case ex: com.datastax.oss.driver.api.core.servererrors.SyntaxError =>
                ZIO.succeed(ZStream(QueryMessageError(ex.getMessage())))
            }
        } yield result
      }

      def makeReceive(queue: Queue[String]): ZPipe[R, Throwable, WebSocketFrame, Unit] = _.mapM {
        case WebSocketFrame.Text(query, _) =>
          queue.offer(query).unit
        case WebSocketFrame.Close(_) =>
          queue.shutdown
      }

      val result = for {
        queue <- zio.ZQueue.bounded[String](10)
        send = ZStream.fromQueue(queue).mapM(handleQuery).flatten.map(qmsg => WebSocketFrame.Text(qmsg.asJson.toString))
        receive = makeReceive(queue)
        result <- ZioWebSocket[R].build(send, receive)
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
  def apply[R <: AppEnv](): CasService[R] = new CasService[R]()
  import RqlService.RqlService

  import castool.configuration._
  import zio.blocking.Blocking
  type Layer0Env = Configuration with Blocking with zio.console.Console
  type Layer1Env = Layer0Env with CassandraConfig with zio.clock.Clock
  type Layer2Env = Layer1Env with CassandraSession// with zio.clock.Clock
  type Layer3Env = Layer2Env with RqlService// with zio.clock.Clock
  type AppEnv = Layer3Env with zio.console.Console

  val layer0: ZLayer[Blocking, Throwable, Layer0Env]  = Blocking.any ++ zio.console.Console.live ++ Configuration.live
  val layer1: ZLayer[Layer0Env, Throwable, Layer1Env] = CassandraConfig.fromConfiguration ++ zio.clock.Clock.live ++ ZLayer.identity
  val layer2: ZLayer[Layer1Env, Throwable, Layer2Env] = CassandraSession.layer ++ zio.clock.Clock.live ++ ZLayer.identity
  val layer3: ZLayer[Layer2Env, Throwable, Layer3Env] = RqlService.layer ++ ZLayer.identity

  val appLayer: ZLayer[ZEnv, Throwable, AppEnv] = layer0 >>> layer1 >>> layer2 >>> layer3
}


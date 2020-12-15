package castool

import cats.effect._
import cats.implicits._
import org.http4s.implicits._
//import org.http4s.server._
import org.http4s.server.blaze.BlazeServerBuilder
import zio._
import zio.clock.Clock
import zio.interop.catz._
import zio.interop.catz.implicits._

import castool.rql.{Lexer, Parser}

object CasTool extends App {
  def run(args: List[String]) = {
    Lexer.main()
    Parser.main()
    ZIO.succeed(zio.ExitCode.success)
    //program.exitCode
  }

  type AppEnv = service.CasService.AppEnv


  //def httpApp[F[_]: Effect: ContextShift] = service.CasService[F]().routes
  def httpApp[R <: AppEnv with zio.console.Console] = service.CasService[R]().routes

  def program = {
    type AppTask[A] = RIO[AppEnv, A]
    type AppTaskIO[A] = zio.IO[Throwable, A]
    val p = for {
      _ <- zio.console.putStrLn("Hello!")
      result <- ZIO.runtime[AppEnv].flatMap { implicit rts =>
        implicit val t: Timer[AppTask] = implicitly[Timer[AppTaskIO]].asInstanceOf[Timer[AppTask]]
        BlazeServerBuilder
          .apply[AppTask](rts.platform.executor.asEC)
          .bindHttp(8080, "0.0.0.0")
          .withHttpApp(httpApp[AppEnv])
          .serve
          .compile[AppTask, AppTask, cats.effect.ExitCode]
          .drain
      }
      _ <- zio.console.putStrLn("Good bye!")
    } yield result
    p.provideSomeLayer(service.CasService.appLayer)
  }
}


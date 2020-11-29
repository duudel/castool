package castool.util

import cats.Applicative
import org.http4s.Response
import org.http4s.server.websocket.WebSocketBuilder
import org.http4s.websocket.WebSocketFrame
import zio._
import zio.stream._
import zio.stream.interop.fs2z._
import zio.interop.catz._

import castool.util.Implicits._

class ZioWebSocket[R] {
  type F[A] = RIO[R, A]
  def build(
    send: ZStream[R, Throwable, WebSocketFrame],
    receive: ZPipe[R, Throwable, WebSocketFrame, Unit]
  ): F[Response[F]] = {
    //WebSocketBuilder[F].build(send.toFs2Stream, ConvPipe[R, WebSocketFrame, Unit](receive).conv)
    WebSocketBuilder[F].build(send.toFs2Stream, receive.toFs2Pipe)
  }
}

object ZioWebSocket {
  def apply[R]: ZioWebSocket[R] = new ZioWebSocket[R]
}



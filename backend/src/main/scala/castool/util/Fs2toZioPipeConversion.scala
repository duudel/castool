package castool.util

import fs2._
import zio._
import zio.stream._
import zio.stream.interop.fs2z._

object Implicits {
  implicit class ConvertPipe[R, I, O](zpipe: ZStream[R, Throwable, I] => ZStream[R, Throwable, O]) {
    type F[A] = RIO[R, A]
    def toFs2Pipe: fs2.Pipe[F, I, O] = {
      inStream: fs2.Stream[F, I] => 
        val zinStream = inStream.toZStream[R]()
        val zoutStream = zpipe(zinStream)
        zoutStream.toFs2Stream
    }
  }
}


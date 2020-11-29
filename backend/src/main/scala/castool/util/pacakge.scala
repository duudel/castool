package castool

import zio.stream._

package object util {
  type ZPipe[R, E, A, B] = ZStream[R, E, A] => ZStream[R, E, B]
}

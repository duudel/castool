package castool

import zio.Has

package object cassandra {
  type CassandraSession = Has[CassandraSession.Service]
}

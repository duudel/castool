package castool.cassandra

import fs2.Stream
import zio._
import zio.interop.catz._
import zio.stream._
//import zio.interop.catz.implicits._

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql

import scala.collection.JavaConverters._

private[cassandra] class CassandraSessionImpl(cqlSession: CqlSession) extends CassandraSession.Service {
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

    def query(q: String): IO[Throwable, (Seq[ColumnDefinition], ZStream[Any, Throwable, ResultRow])] = IO {
      val rs = cqlSession.execute(q)
      val defs = rs
        .getColumnDefinitions()
        .asScala
        .map(ColumnDefinition.fromDriverDef)
        .toSeq
      defs -> extractRows(rs)
    }

    //def query(q: String): IO[Throwable, cql.ResultSet] = IO {
    //  cqlSession.execute(q)
    //}

    def metadata: IO[Throwable, Metadata] = IO {
      Metadata.fromDriverMetadata(cqlSession.getMetadata())
    }

    //def other(q: String): IO[Throwable, AsyncResultSet] = {
    //  val stage = session.executeAsync(q)
    //  ZIO.fromCompletionStage(stage).map(rs => rs.)
    //}
}


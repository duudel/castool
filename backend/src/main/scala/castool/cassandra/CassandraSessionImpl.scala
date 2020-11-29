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
    private def convertRow(index: Long, row: cql.Row): ResultRow = {
      val columnDefs = row.getColumnDefinitions()
      val values = columnDefs.iterator().asScala.zipWithIndex.map {
        case (cd, index) => ColumnValue.fromColumnDefinitionAndRow(cd, row, index)
      }
      ResultRow(index, values.toSeq)
    }

    private def extractRows[R](result: cql.ResultSet): ZStream[R, Throwable, ResultRow] = {
      val columnDefs = result.getColumnDefinitions();
      val stream: ZStream[R, Throwable, ResultRow] = ZStream
        .fromIterator(result.iterator().asScala)
        .zipWithIndex.map { case (row, index) => convertRow(index, row) }
      stream
    }

    /*
    def query(q: String): Task[(Seq[ColumnDefinition], ZStream[Any, Throwable, ResultRow])] = IO {
      val rs = cqlSession.execute(q)
      val defs = rs
        .getColumnDefinitions()
        .asScala
        .map(ColumnDefinition.fromDriverDef)
        .toSeq
      defs -> extractRows(rs)
    }*/
    def query(q: String): zio.IO[Throwable,(Seq[ColumnDefinition], ZStream[Any,Throwable,ResultRow])] = asyncQuery(q)

    private def qstreamTask(rsTask: Task[cql.AsyncResultSet]): Task[ZStream[Any, Throwable, cql.Row]] = {
      rsTask.flatMap { rs =>
        if (rs.remaining() > 0) {
          val stream = ZStream.fromIterator(rs.currentPage().iterator().asScala)
          if (rs.hasMorePages()) {
            val next = ZIO.fromCompletionStage(rs.fetchNextPage())
            qstreamTask(next).map( n =>
              stream.concat(n)
            )
          } else {
            Task(stream)
          }
        } else if (rs.hasMorePages()) {
          val next = ZIO.fromCompletionStage(rs.fetchNextPage())
          qstreamTask(next)
        } else {
          Task(ZStream.empty)
        }
      }
    }

    def asyncQuery(q: String): Task[(Seq[ColumnDefinition], ZStream[Any, Throwable, ResultRow])] = {
      val res = cqlSession.executeAsync(q)
      val rsTask = ZIO.fromCompletionStage(res)
      val columnDefsTask = rsTask.map(_.getColumnDefinitions().asScala.map(ColumnDefinition.fromDriverDef(_)).toSeq)
      for {
        cd <- columnDefsTask
        qs <- qstreamTask(rsTask)
        res = qs.zipWithIndex.map { case (row, index) => convertRow(index, row) }
      } yield (cd, res)
    }

    def metadata: IO[Throwable, Metadata] = IO {
      Metadata.fromDriverMetadata(cqlSession.getMetadata())
    }

}


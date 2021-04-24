package castool.service

import zio._
import zio.duration._
import zio.stream._

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql

import castool.configuration._
import castool.cassandra
import castool.rql
import castool.rql.{Name, SemCheck, SourceDef}
import castool.rql.{AggregationDef, FunctionDef, Value}
import castool.cassandra.ColumnValue
import castool.rql.Execution

object RqlService {
  type RqlService = Has[Service]

  case class ResultRow(num: Long, columns: Seq[rql.Value]) extends Product with Serializable

  case class Error(msg: String, loc: Option[rql.SourceLocation]) extends Product with Serializable
  type Errors = Seq[Error]

  case class QueryResult(
    columnDefs: Seq[(String, rql.ValueType)],
    stream: ZStream[Any, Throwable, ResultRow]
  ) extends Product with Serializable

  trait Service {
    def compile(q: String): IO[Errors, Unit]
    def query(q: String): IO[Errors, QueryResult]
  }

  def convertDataType(ct: ColumnValue.DataType.Value): rql.ValueType = ct match {
    case ColumnValue.DataType.Ascii     => rql.ValueType.Str
    case ColumnValue.DataType.BigInt    => rql.ValueType.Num
    case ColumnValue.DataType.Blob      => rql.ValueType.Str
    case ColumnValue.DataType.Bool      => rql.ValueType.Bool
    case ColumnValue.DataType.Counter   => rql.ValueType.Num
    case ColumnValue.DataType.Date      => rql.ValueType.Str
    case ColumnValue.DataType.Decimal   => rql.ValueType.Num
    case ColumnValue.DataType.Double    => rql.ValueType.Num
    case ColumnValue.DataType.Duration  => rql.ValueType.Str
    case ColumnValue.DataType.Float     => rql.ValueType.Num
    case ColumnValue.DataType.Inet      => rql.ValueType.Str
    case ColumnValue.DataType.Integer   => rql.ValueType.Num
    case ColumnValue.DataType.SmallInt  => rql.ValueType.Num
    case ColumnValue.DataType.Text      => rql.ValueType.Str
    case ColumnValue.DataType.Time      => rql.ValueType.Str
    case ColumnValue.DataType.TimeUuid  => rql.ValueType.Str
    case ColumnValue.DataType.Timestamp => rql.ValueType.Str
    case ColumnValue.DataType.TinyInt   => rql.ValueType.Num
    case ColumnValue.DataType.Uuid      => rql.ValueType.Str
    case ColumnValue.DataType.VarInt    => rql.ValueType.Num
    case ColumnValue.DataType.List      => rql.ValueType.Str
    case ColumnValue.DataType.Map       => rql.ValueType.Obj
    case ColumnValue.DataType.Set       => rql.ValueType.Str
    case ColumnValue.DataType.Tuple     => rql.ValueType.Str
    case ColumnValue.DataType.Udt       => rql.ValueType.Str
  }

  def convertValue(value: ColumnValue): rql.Value = value match {
    case cassandra.ColumnValues.Null          => rql.Null
    case cassandra.ColumnValues.Ascii(v)      => rql.Str(v)
    case cassandra.ColumnValues.BigInt(v)     => rql.Num(v.toDouble)
    case cassandra.ColumnValues.Blob(v)       => rql.Str(v)
    case cassandra.ColumnValues.Bool(v)       => rql.Bool(v)
    case cassandra.ColumnValues.Counter(v)    => rql.Num(v.toDouble)
    case cassandra.ColumnValues.Date(v)       => rql.Str(v.toString) // TODO: add Date to rql
    case cassandra.ColumnValues.Decimal(v)    => rql.Num(v.bigDecimal.doubleValue())
    case cassandra.ColumnValues.Double(v)     => rql.Num(v)
    case cassandra.ColumnValues.Duration(v)   => rql.Str(v)
    case cassandra.ColumnValues.Float(v)      => rql.Num(v)
    case cassandra.ColumnValues.Inet(v)       => rql.Str(v.toString())
    case cassandra.ColumnValues.Integer(v)    => rql.Num(v)
    case cassandra.ColumnValues.SmallInt(v)   => rql.Num(v)
    case cassandra.ColumnValues.Text(v)       => rql.Str(v)
    case cassandra.ColumnValues.Time(v)       => rql.Str(v.toString)
    case cassandra.ColumnValues.TimeUuid(v)   => rql.Str(v.toString) // TODO: add TimeUuid to rql?
    case cassandra.ColumnValues.Timestamp(v)  => rql.Str(v.toString) // TODO: add timestamp to rql
    case cassandra.ColumnValues.TinyInt(v)    => rql.Num(v)
    case cassandra.ColumnValues.Uuid(v)       => rql.Str(v.toString) // TODO: add UUID to rql
    case cassandra.ColumnValues.VarInt(v)     => rql.Num(v.bigInteger.doubleValue())
    case cassandra.ColumnValues.List(v)       => rql.Str(v.mkString(",")) // TODO: add List to rql
    case cassandra.ColumnValues.Map(v)        => rql.Obj(v.map(x => x._1 -> rql.Str(x._2)).toMap)
    case cassandra.ColumnValues.Set(v)        => rql.Str(v.mkString(",")) // TODO: add set or use list
    case cassandra.ColumnValues.Tuple(v)      => rql.Str(v.mkString(",")) // TODO: what to do with tuples?
    case cassandra.ColumnValues.Udt(v)        => rql.Str(v) // TODO: what to do with UDTs?
    case cassandra.ColumnValues.Unknown(v)    => rql.Str(v)
  }

  def convertColumnDefsToSourceDef(columnDefs: Seq[(String, cassandra.ColumnDefinition)]): rql.SourceDef = {
    val sourceDef = columnDefs.map {
      case (columnName, definition) =>
        Name.fromStringUnsafe(columnName) -> convertDataType(definition.dataType)
    }.toSeq
    rql.SourceDef(sourceDef)
  }

  val b64decoder = java.util.Base64.getDecoder()

  val atob = (blob: rql.Str) => {
    val bytes = b64decoder.decode(blob.v)
    rql.Str(new String(bytes))
  }

  class Functions {
    private val functions: scala.collection.mutable.Map[String, rql.FunctionDef[_ <: rql.Value]] = scala.collection.mutable.Map.empty
    private val aggregations: scala.collection.mutable.Map[String, rql.AggregationDef[_ <: rql.Value]] = scala.collection.mutable.Map.empty
    def functionDef(name: rql.Name): Option[rql.FunctionDef[rql.Value]] = functions.get(name.n)
    def aggregationDef(name: rql.Name): Option[rql.AggregationDef[rql.Value]] = aggregations.get(name.n)

    def addFunction[R <: rql.Value](name: rql.Name, parameters: Seq[(String, rql.ValueType)], eval: rql.Eval.Eval[R])(implicit mapper: rql.ValueTypeMapper[R]): Functions = {
      val resultType = mapper.valueType
      val fd = rql.FunctionDef(eval, parameters, resultType)
      functions.+=(name.n -> fd)
      this
    }

    def addAggregation[R <: rql.Value](
      name: rql.Name, parameters: Seq[(String, rql.ValueType)], init: rql.Value, eval: rql.Eval.Eval[R], finalPhase: (rql.Value, rql.Num) => rql.Value
    )(implicit mapper: rql.ValueTypeMapper[R]): Functions = {
      val resultType = mapper.valueType
      val fd = rql.AggregationDef(eval, parameters, resultType, init, finalPhase)
      aggregations.+=(name.n -> fd)
      this
    }
  }

  import rql.ResolveValueType._
  import rql.Name.NameInterpolator

  val builtins = new Functions()
    .addFunction(n"atob", Seq("b" -> rql.ValueType.Str), rql.Eval(atob))

  private class ServiceImpl(casSession: cassandra.CassandraSession.Service) extends Service {
    def compilationEnv: IO[Throwable, rql.Compiler.Env] = {
      def convertTable(table: cassandra.Table): rql.SourceDef = convertColumnDefsToSourceDef(table.columnDefs)
      for {
        metadata <- casSession.metadata
      } yield new rql.Compiler.Env {
        val tables: Map[String, SourceDef] = metadata.keyspaces.flatMap { keyspace =>
          val kstabs: Seq[(String, rql.SourceDef)] = keyspace.tables.toSeq.map {
            case (tableName, table) =>
              val name = keyspace.name + "_" + tableName
              name -> convertTable(table)
          }
          kstabs
        }.toMap

        def tableDef(name: Name): Option[SourceDef] = {
          tables.get(name.n)
        }

        def functionDef(name: Name): Option[FunctionDef[Value]] = builtins.functionDef(name)
        def aggregationDef(name: Name): Option[AggregationDef[Value]] = builtins.aggregationDef(name)
      }
    }

    def executionEnv = {
      for {
        metadata <- casSession.metadata
          .mapError(t => Error(t.getMessage(), None) :: Nil)
      } yield new rql.Execution.Env {
        val tables: Map[String, (cassandra.Keyspace, cassandra.Table)] = metadata.keyspaces.flatMap { keyspace =>
          val kstabs: Seq[(String, (cassandra.Keyspace, cassandra.Table))] = keyspace.tables.toSeq.map {
            case (tableName, table) =>
              val name = keyspace.name + "_" + tableName
              name -> (keyspace -> table)
          }
          kstabs
        }.toMap

        def table(name: Name): Option[Execution.Stream] = {
          tables.get(name.n) match {
            case None => None
            case Some((keyspace, tab)) =>
              val tableName = keyspace.name + "." + tab.name
              val q = s"SELECT ${tab.columnDefs.map(_._1).mkString(", ")} FROM $tableName"
              val effect = for {
                queryResult <- casSession.query(q)
              } yield {
                val sourceDef = convertColumnDefsToSourceDef(queryResult.columnDefs.map(c => c.name -> c))
                queryResult.rowsStream.map { resultRow =>
                  val values = sourceDef.columns
                    .zip(resultRow.columnValues)
                    .map { case ((name, valueType), columnValue) =>
                      name -> convertValue(columnValue)
                    }
                  rql.InputRow(values)
                }
              }
              val stream = ZStream.fromEffect(effect).flatMap(identity)
              Some(stream)
          }
        }
      }
    }

    def compile(q: String): zio.IO[Errors, Unit] = {
      val result = for {
        env <- compilationEnv.mapError(t => Error(t.getMessage(), None) :: Nil)
        compiled <- rql.Compiler.compileQuery(q).provide(env)
          .mapError(errors => errors.map(err =>
              Error(err.message, Some(err.loc)))
          )
      } yield compiled

      result.as(())
    }

    def query(q: String): zio.IO[Errors, QueryResult] = {
      val result = for {
        env <- compilationEnv.mapError(t => Error(t.getMessage(), None) :: Nil)
        compiled <- rql.Compiler.compileQuery(q).provide(env)
          .mapError(errors => errors.map(err =>
              Error(err.message, Some(err.loc)))
          )
        execEnv <- executionEnv
        live = rql.Execution.live(execEnv)
        stream = rql.Execution.run(compiled)
          .provideLayer(live)
      } yield {
        val columnDefs = compiled.sourceDef.columns.map {
          case (name, valueType) =>
            name.n -> valueType
        }.toVector
        val resultStream = stream.zipWithIndex.map {
          case (inputRow, index) => ResultRow(index, inputRow.values.values.toVector)
        }
        QueryResult(columnDefs, resultStream)
      }

      result
    }
  }

  def layer: ZLayer[cassandra.CassandraSession, Throwable, RqlService] = {
    ZLayer.fromEffect(
      for {
        cs <- ZIO.access[cassandra.CassandraSession](_.get)
        metadata <- cs.metadata
      } yield new ServiceImpl(cs)
    )
  }

  def query(q: String): ZIO[RqlService, Errors, QueryResult] = ZIO.accessM[RqlService](_.get.query(q))

}


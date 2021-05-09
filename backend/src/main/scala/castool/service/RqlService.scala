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

  def convertDataType(ct: ColumnValue.DataType): rql.ValueType = ct.code match {
    case ColumnValue.DataTypeCode.Ascii     => rql.ValueType.Str
    case ColumnValue.DataTypeCode.BigInt    => rql.ValueType.Num
    case ColumnValue.DataTypeCode.Blob      => rql.ValueType.Blob
    case ColumnValue.DataTypeCode.Bool      => rql.ValueType.Bool
    case ColumnValue.DataTypeCode.Counter   => rql.ValueType.Num
    case ColumnValue.DataTypeCode.Date      => rql.ValueType.Date
    case ColumnValue.DataTypeCode.Decimal   => rql.ValueType.Num
    case ColumnValue.DataTypeCode.Double    => rql.ValueType.Num
    case ColumnValue.DataTypeCode.Duration  => rql.ValueType.Str
    case ColumnValue.DataTypeCode.Float     => rql.ValueType.Num
    case ColumnValue.DataTypeCode.Inet      => rql.ValueType.Str
    case ColumnValue.DataTypeCode.Integer   => rql.ValueType.Num
    case ColumnValue.DataTypeCode.SmallInt  => rql.ValueType.Num
    case ColumnValue.DataTypeCode.Text      => rql.ValueType.Str
    case ColumnValue.DataTypeCode.Time      => rql.ValueType.Str
    case ColumnValue.DataTypeCode.TimeUuid  => rql.ValueType.Str
    case ColumnValue.DataTypeCode.Timestamp => rql.ValueType.Date
    case ColumnValue.DataTypeCode.TinyInt   => rql.ValueType.Num
    case ColumnValue.DataTypeCode.Uuid      => rql.ValueType.Str
    case ColumnValue.DataTypeCode.VarInt    => rql.ValueType.Num
    case ColumnValue.DataTypeCode.List      => rql.ValueType.List
    case ColumnValue.DataTypeCode.Map       => rql.ValueType.Obj
    case ColumnValue.DataTypeCode.Set       => rql.ValueType.List
    case ColumnValue.DataTypeCode.Tuple     => rql.ValueType.Str
    case ColumnValue.DataTypeCode.Udt       => rql.ValueType.Str
  }

  def convertCollectionValue(x: Any): rql.Value = x match {
    case b: Boolean => rql.Bool(b)
    case s: String => rql.Str(s)
    case n: Byte => rql.Num(n)
    case n: Short => rql.Num(n)
    case n: Int => rql.Num(n)
    case n: Long => rql.Num(n)
    case n: Double => rql.Num(n)
  }

  def convertValue(value: ColumnValue): rql.Value = value match {
    case cassandra.ColumnValues.Null          => rql.Null
    case cassandra.ColumnValues.Ascii(v)      => rql.Str(v)
    case cassandra.ColumnValues.BigInt(v)     => rql.Num(v.toDouble)
    case cassandra.ColumnValues.Blob(v)       => rql.Blob(v)
    case cassandra.ColumnValues.Bool(v)       => rql.Bool(v)
    case cassandra.ColumnValues.Counter(v)    => rql.Num(v.toDouble)
    case cassandra.ColumnValues.Date(v)       => rql.Date(v.atStartOfDay().atOffset(java.time.ZoneOffset.UTC).toInstant()) // TODO: add Date to RQL, change current Date to Timestamp?
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
    case cassandra.ColumnValues.Timestamp(v)  => rql.Date(v) // TODO: add timestamp to rql
    case cassandra.ColumnValues.TinyInt(v)    => rql.Num(v)
    case cassandra.ColumnValues.Uuid(v)       => rql.Str(v.toString) // TODO: add UUID to rql
    case cassandra.ColumnValues.VarInt(v)     => rql.Num(v.bigInteger.doubleValue())
    case cassandra.ColumnValues.List(v)       => rql.List(v.map(convertCollectionValue))
    case cassandra.ColumnValues.Map(v)        =>
      rql.Obj(v.mapValues(convertCollectionValue).map {
        case (k, v) => k.toString -> v
      }.toMap)
    case cassandra.ColumnValues.Set(v)        => rql.List(v.toVector.map(convertCollectionValue)) // TODO: add set data type?
    case cassandra.ColumnValues.Tuple(v)      => rql.Str(v.mkString(",")) // TODO: what to do with tuples?
    case cassandra.ColumnValues.Udt(v)        => rql.Str(v) // TODO: what to do with UDTs?
    case cassandra.ColumnValues.Unknown(v)    => rql.Str(v)
  }

  def convertColumnDefsToSourceDef(columnDefs: Seq[cassandra.ColumnDefinition]): rql.SourceDef = {
    val sourceDef = columnDefs.map { definition =>
      Name.fromStringUnsafe(definition.name) -> convertDataType(definition.dataType)
    }.toSeq
    rql.SourceDef(sourceDef)
  }

  val b64decoder = java.util.Base64.getDecoder()
  val b64encoder = java.util.Base64.getEncoder()

  val atob = (str: rql.Str) => {
    val bytes = b64decoder.decode(str.v)
    rql.Str(new String(bytes))
  }

  val btoa = (str: rql.Str) => {
    val bytes = b64encoder.encode(str.v.getBytes())
    rql.Str(new String(bytes))
  }

  val blobAsString = (blob: rql.Blob) => {
    rql.Str(new String(blob.v))
  }

  val uuid_to_date = (v: rql.Str) => {
    val NUM_100NS_INTERVALS_SINCE_UUID_EPOCH_UNTIL_INSTANT_EPOCH = 0x01b21dd213814000L
    try {
      val uuid = java.util.UUID.fromString(v.v)
      val ts = uuid.timestamp()
      val millisAfterInstantEpoch = (ts - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH_UNTIL_INSTANT_EPOCH) / 10000
      val instant = java.time.Instant.ofEpochMilli(millisAfterInstantEpoch)
      rql.Date(instant)
    } catch {
      case ex: Throwable => rql.Null.asInstanceOf[Nothing]
    }
  }

  object JsonToRqlFolder extends io.circe.Json.Folder[rql.Value] {
    def onNull: rql.Value = rql.Null
    def onBoolean(value: Boolean): rql.Value = rql.Bool(value)
    def onNumber(value: io.circe.JsonNumber): rql.Value = rql.Num(value.toDouble)
    def onString(value: String): rql.Value = {
      rql.Date
        .fromString(value)
        .getOrElse(rql.Str(value))
    }
    def onArray(value: Vector[io.circe.Json]): rql.Value = rql.List(value.map(convertJsonToRql))
    def onObject(value: io.circe.JsonObject): rql.Value = jsonObjectToRql(value)
  }
  def convertJsonToRql(json: io.circe.Json): rql.Value = {
    json.foldWith(JsonToRqlFolder)
  }

  def jsonObjectToRql(jsonObj: io.circe.JsonObject): rql.Obj = {
    val v = jsonObj.toMap.map {
      case (key, value) =>
        key -> convertJsonToRql(value)
    }
    rql.Obj(v)
  }

  val jsonToObject: rql.Str => rql.Obj = (v: rql.Str) => {
    io.circe.parser.parse(v.v) match {
      case Right(json) =>
        json.asObject.map { jsonObj =>
          jsonObjectToRql(jsonObj)
        }.getOrElse(rql.Null.asInstanceOf[rql.Obj])
      case Left(_) => rql.Null.asInstanceOf[rql.Obj]
    }
  }

  import rql.ResolveValueType._
  import rql.Name.NameInterpolator

  val builtins = new rql.Functions()
    .addFunction(n"atob", Seq("encodedData" -> rql.ValueType.Str), rql.Eval(atob))
    .addFunction(n"btoa", Seq("stringToEncode" -> rql.ValueType.Str), rql.Eval(btoa))
    .addFunction(n"blobAsString", Seq("blob" -> rql.ValueType.Blob), rql.Eval(blobAsString))
    .addFunction(n"uuidToDate", Seq("uuid" -> rql.ValueType.Str), rql.Eval(uuid_to_date))
    .addFunction(n"length", Seq("str" -> rql.ValueType.Str), rql.Eval((str: rql.Str) => rql.Num(str.v.length)))
    .addFunction(n"length", Seq("blob" -> rql.ValueType.Blob), rql.Eval((blob: rql.Blob) => rql.Num(blob.v.length)))
    .addFunction(n"length", Seq("list" -> rql.ValueType.List), rql.Eval((list: rql.List) => rql.Num(list.v.length)))
    .addFunction(n"bin", Seq("value" -> rql.ValueType.Num, "roundTo" -> rql.ValueType.Num), rql.Eval((value: rql.Num, roundTo: rql.Num) => rql.Num((value.v/roundTo.v).round * roundTo.v)))
    .addFunction(n"jsonToObject", ("value", rql.ValueType.Str) :: Nil, rql.Eval(jsonToObject))
    .addAggregation[rql.Num](n"count", Seq("x" -> rql.ValueType.Num), rql.Num(0), rql.Eval(() => rql.Num(1)), (x, num) => num)
    .addAggregation(n"average",
      parameters = Seq("acc" -> rql.ValueType.Num, "x" -> rql.ValueType.Num),
      init = rql.Num(0),
      eval = rql.Eval((acc: rql.Num, x: rql.Num) => rql.Num(acc.v + x.v)),
      finalPhase = (result: rql.Num, num) => rql.Num(result.v / num.v)
    )

  private class ServiceImpl(casSession: cassandra.CassandraSession.Service) extends Service {
    def compilationEnv: IO[Throwable, rql.Compiler.Env] = {
      def convertTable(table: cassandra.Table): rql.SourceDef = convertColumnDefsToSourceDef(table.columnDefs)
      for {
        metadata <- casSession.metadata
      } yield new rql.Compiler.Env {
        val tables: Map[String, SourceDef] = metadata.keyspaces.flatMap { keyspace =>
          val kstabs: Seq[(String, rql.SourceDef)] = keyspace.tables.toSeq.map { table =>
            val name = keyspace.name + "_" + table.name
            name -> convertTable(table)
          }
          kstabs
        }.toMap

        def tableDef(name: Name): Option[SourceDef] = {
          tables.get(name.n)
        }

        def functionDef(name: Name, argumentTypes: Seq[rql.ValueType]): Option[FunctionDef[Value]] = builtins.functionDef(name, argumentTypes)
        def aggregationDef(name: Name): Option[AggregationDef[Value]] = builtins.aggregationDef(name)
      }
    }

    def executionEnv = {
      for {
        metadata <- casSession.metadata
          .mapError(t => Error(t.getMessage(), None) :: Nil)
      } yield new rql.Execution.Env {
        val tables: Map[String, (cassandra.Keyspace, cassandra.Table)] = metadata.keyspaces.flatMap { keyspace =>
          val kstabs: Seq[(String, (cassandra.Keyspace, cassandra.Table))] = keyspace.tables.toSeq.map { table =>
            val name = keyspace.name + "_" + table.name
            name -> (keyspace -> table)
          }
          kstabs
        }.toMap

        def table(name: Name): Option[Execution.Stream] = {
          tables.get(name.n) match {
            case None => None
            case Some((keyspace, tab)) =>
              val tableName = keyspace.name + "." + tab.name
              val q = s"SELECT ${tab.columnDefs.map(_.name).mkString(", ")} FROM $tableName"
              val effect = for {
                queryResult <- casSession.query(q)
              } yield {
                val sourceDef = convertColumnDefsToSourceDef(queryResult.columnDefs)
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


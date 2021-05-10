package castool.cassandra

import cats.syntax.functor._
import io.circe.{Decoder, Encoder}
import io.circe.generic.auto._
import io.circe.generic.semiauto._
import io.circe.syntax._
import io.circe.HCursor
import io.circe.DecodingFailure
import io.circe.Json

import com.datastax.oss.driver.api.core.cql
import com.datastax.oss.driver.api.core.`type`.{DataType => CasDataType}
import java.net.InetAddress

import scala.util.Try
import scala.jdk.CollectionConverters._
import java.util.Base64

sealed trait ColumnValue

object ColumnValue {
  import ColumnValues._

  //implicit val decoder: Decoder[ColumnValue] = deriveDecoder
  //implicit val encoder: Encoder[ColumnValue] = deriveEncoder
  private def convertAny(x: Any): Json = x match {
    case b: Boolean => b.asJson
    case s: String => s.asJson
    case n: Byte => n.asJson
    case n: Short => n.asJson
    case n: Int => n.asJson
    case n: Long => n.asJson
    case n: Double => n.asJson
  }
  implicit val encoder: Encoder[ColumnValue] = (v: ColumnValue) => v match {
    case Null => Json.Null
    case Unknown(v: String) => Json.fromString(v)

    case Ascii(v: String) => Json.fromString(v)
    case BigInt(v: Long) => Json.fromLong(v)
    case Blob(v: Array[Byte]) =>
      val s = Base64.getEncoder.encodeToString(v)
      Json.fromString(s)
    case Bool(v: Boolean) => if (v) Json.True else Json.False
    case Counter(v: Long) => Json.fromLong(v)
    case Decimal(v: scala.BigDecimal) => Json.fromBigDecimal(v)
    case Double(v: scala.Double) => Json.fromDouble(v).getOrElse(Json.fromInt(0))
    case Float(v: scala.Float) => Json.fromFloat(v).getOrElse(Json.fromInt(0))
    case Integer(v: Int) => Json.fromInt(v)
    case SmallInt(v: Short) => Json.fromInt(v)
    case TinyInt(v: Byte) => Json.fromInt(v)
    case Timestamp(v: java.time.Instant) => Json.fromString(v.toString)
    case Uuid(v: java.util.UUID) => Json.fromString(v.toString)
    case VarInt(v: scala.BigInt) => Json.fromBigInt(v)
    case TimeUuid(v: java.util.UUID) => Json.fromString(v.toString)
    case Text(v: String) => Json.fromString(v)
    case Inet(v: InetAddress) => Json.fromString(v.getHostAddress())
    case Date(v: java.time.LocalDate) => Json.fromString(v.toString)
    case Time(v: java.time.LocalTime) => Json.fromString(v.toString)
    case Duration(v: String) => Json.fromString(v)
    case List(v: Vector[Any]) => Json.fromValues(v.map(convertAny))
    case Set(v: scala.collection.Set[Any]) => Json.fromValues(v.map(convertAny))
    case Map(v: scala.collection.Map[Any, Any]) => Json.fromFields(v.map { case (key, value) => key.toString -> convertAny(value) })
    case Tuple(v: String) => Json.fromString(v)
    case Udt(v: String) => Json.fromString(v)
  }

  def fromValue[C <: ColumnValue, T](f: T => C)(v: T): ColumnValue =
    Option(v).map(f).getOrElse(Null)

  def fromColumnDefinitionAndRow(columnDef: cql.ColumnDefinition, row: cql.Row, index: Int): ColumnValue = {
    import com.datastax.oss.protocol.internal.ProtocolConstants

    def convertGeneric[R](protocolCode: Int)(convertX: Class[_] => R) = {
      protocolCode match {
        case ProtocolConstants.DataType.ASCII | ProtocolConstants.DataType.VARCHAR => convertX(classOf[String])
        case ProtocolConstants.DataType.BOOLEAN => convertX(classOf[Boolean])
        case ProtocolConstants.DataType.INT => convertX(classOf[java.lang.Integer])
        case ProtocolConstants.DataType.SMALLINT => convertX(classOf[java.lang.Short])
        case ProtocolConstants.DataType.TINYINT => convertX(classOf[java.lang.Byte])
        case ProtocolConstants.DataType.BIGINT => convertX(classOf[java.lang.Long])
        case ProtocolConstants.DataType.TIMESTAMP => convertX(classOf[java.time.Instant])
        case ProtocolConstants.DataType.UUID => convertX(classOf[java.util.UUID])
        case ProtocolConstants.DataType.VARINT => convertX(classOf[java.math.BigInteger])
        case ProtocolConstants.DataType.TIMEUUID => convertX(classOf[java.util.UUID])
        case ProtocolConstants.DataType.INET => convertX(classOf[java.net.InetAddress])
        case ProtocolConstants.DataType.DATE => convertX(classOf[java.time.LocalDate])
        case ProtocolConstants.DataType.TIME => convertX(classOf[java.time.LocalTime])
        //case ProtocolConstants.DataType.DURATION =>
      }
    }

    columnDef.getType().getProtocolCode() match {
      case ProtocolConstants.DataType.ASCII => fromValue(Ascii)(row.getString(index))
      case ProtocolConstants.DataType.BIGINT => fromValue(BigInt)(row.get(index, classOf[Long]))
      case ProtocolConstants.DataType.BLOB =>
        val buf = row.getByteBuffer(index)
        if (buf == null) {
          Null
        } else {
          Blob(buf.array())
        }
      case ProtocolConstants.DataType.BOOLEAN => fromValue(Bool)(row.get(index, classOf[Boolean]))
      case ProtocolConstants.DataType.COUNTER => fromValue(Counter)(row.get(index, classOf[Long]))
      case ProtocolConstants.DataType.DECIMAL => fromValue(Decimal)(row.getBigDecimal(index))
      case ProtocolConstants.DataType.DOUBLE => fromValue(Double)(row.get(index, classOf[scala.Double]))
      case ProtocolConstants.DataType.FLOAT => fromValue(Float)(row.get(index, classOf[scala.Float]))
      case ProtocolConstants.DataType.INT => fromValue(Integer)(row.get(index, classOf[Int]))
      case ProtocolConstants.DataType.SMALLINT => fromValue(SmallInt)(row.get(index, classOf[Short]))
      case ProtocolConstants.DataType.TINYINT => fromValue(TinyInt)(row.get(index, classOf[Byte]))
      case ProtocolConstants.DataType.TIMESTAMP => fromValue(Timestamp)(row.getInstant(index))
      case ProtocolConstants.DataType.UUID => fromValue(Uuid)(row.getUuid(index))
      case ProtocolConstants.DataType.VARINT => fromValue(VarInt)(row.getBigInteger(index))
      case ProtocolConstants.DataType.TIMEUUID => fromValue(TimeUuid)(row.getUuid(index))
      case ProtocolConstants.DataType.VARCHAR => fromValue(Text)(row.getString(index))
      case ProtocolConstants.DataType.INET => fromValue(Inet)(row.getInetAddress(index))
      case ProtocolConstants.DataType.DATE => fromValue(Date)(row.getLocalDate(index))
      case ProtocolConstants.DataType.TIME => fromValue(Time)(row.getLocalTime(index))
      case ProtocolConstants.DataType.DURATION =>
        val dur = row.getCqlDuration(index)
        if (dur == null) {
          Null
        } else {
          fromValue(Duration)(dur.toString())
        }
      case ProtocolConstants.DataType.LIST =>
        if (row.isNull(index)) {
          Null
        } else {
          val listType = columnDef.getType().asInstanceOf[com.datastax.oss.driver.api.core.`type`.ListType]
          convertGeneric(listType.getElementType().getProtocolCode()) { elemClass =>
            val list = row.getList(index, elemClass)
            if (list == null) {
              List(Vector.empty)
            } else {
              fromValue(List)(list.asScala.toVector)
            }
          }
        }
      case ProtocolConstants.DataType.MAP =>
        if (row.isNull(index)) {
          Null
        } else {
          val mapType = columnDef.getType().asInstanceOf[com.datastax.oss.driver.api.core.`type`.MapType]
          convertGeneric(mapType.getKeyType().getProtocolCode()) { keyClass =>
            convertGeneric(mapType.getValueType().getProtocolCode()) { valueClass =>
              val map = row.getMap(index, keyClass, valueClass)
              if (map == null) Map(scala.collection.Map.empty) else Map(map.asScala.toMap)
            }
          }
        }
      case ProtocolConstants.DataType.SET =>
        if (row.isNull(index)) {
          Null
        } else {
          val setType = columnDef.getType().asInstanceOf[com.datastax.oss.driver.api.core.`type`.SetType]
          convertGeneric(setType.getElementType().getProtocolCode()) { elemClass =>
            val set = row.getSet(index, elemClass)
            if (set == null) {
              Set(scala.collection.Set.empty)
            } else {
              fromValue(Set)(set.asScala.toSet)
            }
          }
        }
      case ProtocolConstants.DataType.TUPLE =>
        val tuple = row.getTupleValue(index)
        if (tuple == null) {
          Null
        } else {
          fromValue(Tuple)(tuple.getFormattedContents)
        }
      case ProtocolConstants.DataType.UDT =>
        val value = row.getUdtValue(index)
        if (value == null) {
          Null
        } else {
          fromValue(Udt)(value.getFormattedContents)
        }

      case _ =>
        Unknown(columnDef.getName().asCql(true))
    }
  }

  sealed trait DataType { def code: DataTypeCode }
  final case class PrimitiveType(code: DataTypeCode) extends DataType
  final case class ListType(elem: DataType) extends DataType {
    val code: DataTypeCode = DataTypeCode.List
  }
  final case class SetType(elem: DataType) extends DataType {
    val code: DataTypeCode = DataTypeCode.Set
  }
  final case class MapType(key: DataType, value: DataType) extends DataType {
    val code: DataTypeCode = DataTypeCode.Map
  }

  object DataType {
    def fromCas(dt: CasDataType): DataType = {
      import com.datastax.oss.protocol.internal.ProtocolConstants
      import DataTypeCode._

      dt.getProtocolCode() match {
        case ProtocolConstants.DataType.ASCII => PrimitiveType(Ascii)
        case ProtocolConstants.DataType.BIGINT => PrimitiveType(BigInt)
        case ProtocolConstants.DataType.BLOB => PrimitiveType(Blob)
        case ProtocolConstants.DataType.BOOLEAN => PrimitiveType(Bool)
        case ProtocolConstants.DataType.COUNTER => PrimitiveType(Counter)
        case ProtocolConstants.DataType.DECIMAL => PrimitiveType(Decimal)
        case ProtocolConstants.DataType.DOUBLE => PrimitiveType(Double)
        case ProtocolConstants.DataType.FLOAT => PrimitiveType(Float)
        case ProtocolConstants.DataType.INT => PrimitiveType(Integer)
        case ProtocolConstants.DataType.SMALLINT => PrimitiveType(SmallInt)
        case ProtocolConstants.DataType.TINYINT => PrimitiveType(TinyInt)
        case ProtocolConstants.DataType.TIMESTAMP => PrimitiveType(Timestamp)
        case ProtocolConstants.DataType.UUID => PrimitiveType(Uuid)
        case ProtocolConstants.DataType.VARINT => PrimitiveType(VarInt)
        case ProtocolConstants.DataType.TIMEUUID => PrimitiveType(TimeUuid)
        case ProtocolConstants.DataType.VARCHAR => PrimitiveType(Text)
        case ProtocolConstants.DataType.INET => PrimitiveType(Inet)
        case ProtocolConstants.DataType.DATE => PrimitiveType(Date)
        case ProtocolConstants.DataType.TIME => PrimitiveType(Time)
        case ProtocolConstants.DataType.DURATION => PrimitiveType(Duration)
        case ProtocolConstants.DataType.LIST =>
          val listType = dt.asInstanceOf[com.datastax.oss.driver.api.core.`type`.ListType]
          val elemType = DataType.fromCas(listType.getElementType())
          ListType(elemType)
        case ProtocolConstants.DataType.MAP =>
          val mapType = dt.asInstanceOf[com.datastax.oss.driver.api.core.`type`.MapType]
          val keyType = DataType.fromCas(mapType.getKeyType())
          val valueType = DataType.fromCas(mapType.getValueType())
          MapType(keyType, valueType)
        case ProtocolConstants.DataType.SET =>
          val setType = dt.asInstanceOf[com.datastax.oss.driver.api.core.`type`.SetType]
          val elemType = DataType.fromCas(setType.getElementType())
          SetType(elemType)
        case ProtocolConstants.DataType.TUPLE => PrimitiveType(Tuple)
        case ProtocolConstants.DataType.UDT => PrimitiveType(Udt)
      }
    }

    implicit val encoder: Encoder[DataType] = (dt: DataType) => dt match {
      case PrimitiveType(code) => Json.obj("code" -> code.asJson)
      case ListType(elem) => Json.obj("code" -> dt.code.asJson, "elem" -> elem.asJson)
      case SetType(elem) => Json.obj("code" -> dt.code.asJson, "elem" -> elem.asJson)
      case MapType(key, value) => Json.obj("code" -> dt.code.asJson, "key" -> key.asJson, "value" -> value.asJson)
    }
  }

  object DataTypeCode extends Enumeration {
    val Ascii = Value
    val BigInt = Value
    val Blob = Value
    val Bool = Value
    val Counter = Value
    val Decimal = Value
    val Double = Value
    val Float = Value
    val Integer = Value
    val SmallInt = Value
    val TinyInt = Value
    val Timestamp = Value
    val Uuid = Value
    val VarInt = Value
    val TimeUuid = Value
    val Text = Value
    val Inet = Value
    val Date = Value
    val Time = Value
    val Duration = Value
    val List = Value
    val Map = Value
    val Set = Value
    val Tuple = Value
    val Udt = Value

    implicit val encoder: Encoder[DataTypeCode] = Encoder.enumEncoder(DataTypeCode)
    implicit val decoder: Decoder[DataTypeCode] = Decoder.enumDecoder(DataTypeCode)

    def fromCas(dt: CasDataType): DataTypeCode = {
      import com.datastax.oss.protocol.internal.ProtocolConstants

      dt.getProtocolCode() match {
        case ProtocolConstants.DataType.ASCII => Ascii
        case ProtocolConstants.DataType.BIGINT => BigInt
        case ProtocolConstants.DataType.BLOB => Blob
        case ProtocolConstants.DataType.BOOLEAN => Bool
        case ProtocolConstants.DataType.COUNTER => Counter
        case ProtocolConstants.DataType.DECIMAL => Decimal
        case ProtocolConstants.DataType.DOUBLE => Double
        case ProtocolConstants.DataType.FLOAT => Float
        case ProtocolConstants.DataType.INT => Integer
        case ProtocolConstants.DataType.SMALLINT => SmallInt
        case ProtocolConstants.DataType.TINYINT => TinyInt
        case ProtocolConstants.DataType.TIMESTAMP => Timestamp
        case ProtocolConstants.DataType.UUID => Uuid
        case ProtocolConstants.DataType.VARINT => VarInt
        case ProtocolConstants.DataType.TIMEUUID => TimeUuid
        case ProtocolConstants.DataType.VARCHAR => Text
        case ProtocolConstants.DataType.INET => Inet
        case ProtocolConstants.DataType.DATE => Date
        case ProtocolConstants.DataType.TIME => Time
        case ProtocolConstants.DataType.DURATION => Duration
        case ProtocolConstants.DataType.LIST => List
        case ProtocolConstants.DataType.MAP => Map
        case ProtocolConstants.DataType.SET => Set
        case ProtocolConstants.DataType.TUPLE => Tuple
        case ProtocolConstants.DataType.UDT => Udt
      }
    }
  }
  type DataTypeCode = DataTypeCode.Value
}

object ColumnValues {
  case object Null extends ColumnValue
  case class Unknown(v: String) extends ColumnValue

  case class Ascii(v: String) extends ColumnValue // ProtocolConstants.DataType.ASCII
  case class BigInt(v: Long) extends ColumnValue // ProtocolConstants.DataType.BIGINT
  case class Blob(v: Array[Byte]) extends ColumnValue // ProtocolConstants.DataType.BLOB
  case class Bool(v: Boolean) extends ColumnValue // ProtocolConstants.DataType.BOOLEAN
  case class Counter(v: Long) extends ColumnValue // ProtocolConstants.DataType.COUNTER
  case class Decimal(v: scala.BigDecimal) extends ColumnValue // ProtocolConstants.DataType.DECIMAL
  case class Double(v: scala.Double) extends ColumnValue // ProtocolConstants.DataType.DOUBLE
  case class Float(v: scala.Float) extends ColumnValue // ProtocolConstants.DataType.FLOAT
  case class Integer(v: Int) extends ColumnValue // ProtocolConstants.DataType.INT
  case class SmallInt(v: Short) extends ColumnValue // ProtocolConstants.DataType.SMALLINT
  case class TinyInt(v: Byte) extends ColumnValue // ProtocolConstants.DataType.TINYINT
  case class Timestamp(v: java.time.Instant) extends ColumnValue // ProtocolConstants.DataType.TIMESTAMP
  case class Uuid(v: java.util.UUID) extends ColumnValue // ProtocolConstants.DataType.UUID
  case class VarInt(v: scala.BigInt) extends ColumnValue // ProtocolConstants.DataType.VARINT
  case class TimeUuid(v: java.util.UUID) extends ColumnValue // ProtocolConstants.DataType.TIMEUUID
  case class Text(v: String) extends ColumnValue // ProtocolConstants.DataType.VARCHAR
  case class Inet(v: InetAddress) extends ColumnValue // ProtocolConstants.DataType.INET
  case class Date(v: java.time.LocalDate) extends ColumnValue // ProtocolConstants.DataType.DATE
  case class Time(v: java.time.LocalTime) extends ColumnValue // ProtocolConstants.DataType.TIME
  case class Duration(v: String) extends ColumnValue // ProtocolConstants.DataType.DURATION
  case class List(v: Vector[Any]) extends ColumnValue // ProtocolConstants.DataType.LIST
  case class Map(v: scala.collection.Map[Any, Any]) extends ColumnValue // ProtocolConstants.DataType.MAP
  case class Set(v: scala.collection.Set[Any]) extends ColumnValue // ProtocolConstants.DataType.SET
  case class Tuple(v: String) extends ColumnValue // ProtocolConstants.DataType.TUPLE
  case class Udt(v: String) extends ColumnValue // ProtocolConstants.DataType.UDT

  implicit val inetDecoder: Decoder[Inet] = (cursor: HCursor) => for {
    s <- cursor.as[String]
    result <- Try(Inet(InetAddress.getByName(s)))
      .toEither.left.map { ex => DecodingFailure(s"${ex.getMessage()}", cursor.history) }
  } yield result

  implicit val inetEncoder: Encoder[Inet] = (inet: Inet) => Json.fromString(inet.v.getHostAddress())

  // Long decoded from string and encoded to string
  implicit val longDecoder: Decoder[Long] = (cursor: HCursor) => for {
    s <- cursor.as[String]
    result <- Try(s.toLong)
      .toEither.left.map { ex => DecodingFailure(s"${ex.getMessage()}", cursor.history) }
  } yield result

  implicit val longEncoder: Encoder[Long] = (x: Long) => Json.fromString(x.toString)

  implicit val blobEncoder: Encoder[Blob] = (blob: Blob) => {
    val s = Base64.getEncoder.encodeToString(blob.v)
    Json.obj("v" -> s.asJson)
  }

  implicit val listEncoder: Encoder[Vector[Any]] = (m: Vector[Any]) => {
    val mapped = m.map {
      case b: Boolean => b.asJson
      case s: String => s.asJson
      case n: Int => n.asJson
      case n: Double => n.asJson
    }

    Json.arr(mapped: _*)
  }

  implicit val setEncoder: Encoder[scala.collection.Set[Any]] = (m: scala.collection.Set[Any]) => {
    val mapped = m.map {
      case b: Boolean => b.asJson
      case s: String => s.asJson
      case n: Int => n.asJson
      case n: Long => n.asJson
      case n: Double => n.asJson
    }

    Json.arr(mapped.toSeq: _*)
  }

  implicit val mapEncoder: Encoder[scala.collection.Map[Any, Any]] = (m: scala.collection.Map[Any, Any]) => {
    val mapped = m.mapValues {
      case b: Boolean => b.asJson
      case s: String => s.asJson
      case n: Int => n.asJson
      case n: Long => n.asJson
      case n: Double => n.asJson
    }

    Json.obj(mapped.map { case (k, v) => k.toString -> v }.toVector: _*)
  }
}


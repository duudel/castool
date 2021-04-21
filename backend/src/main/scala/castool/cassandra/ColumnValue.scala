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
import java.util.Base64
import java.net.InetAddress

import scala.util.Try
import scala.jdk.CollectionConverters._

sealed trait ColumnValue

object ColumnValue {
  import ColumnValues._

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

  implicit val decoder: Decoder[ColumnValue] = deriveDecoder

  def fromValue[C <: ColumnValue, T](f: T => C)(v: T): ColumnValue =
    Option(v).map(f).getOrElse(Null)

  def fromColumnDefinitionAndRow(columnDef: cql.ColumnDefinition, row: cql.Row, index: Int): ColumnValue = {
    columnDef.getType().getProtocolCode() match {
      case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.ASCII =>
        fromValue(Ascii)(row.getString(index))
      case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.BIGINT =>
        fromValue(BigInt)(row.get(index, classOf[Long]))
      case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.BLOB =>
        val buf = row.getByteBuffer(index)
        if (buf == null) {
          Null
        } else {
          val encoder = Base64.getEncoder()
          val b64 = encoder.encode(buf)
          Blob(new String(b64.array()))
        }
      case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.BOOLEAN =>
        fromValue(Bool)(row.get(index, classOf[Boolean]))
      case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.COUNTER =>
        fromValue(Counter)(row.get(index, classOf[Long]))
      case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.DECIMAL =>
        fromValue(Decimal)(row.getBigDecimal(index))
      case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.DOUBLE =>
        fromValue(Double)(row.get(index, classOf[scala.Double]))
      case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.FLOAT =>
        fromValue(Float)(row.get(index, classOf[scala.Float]))
      case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.INT =>
        fromValue(Integer)(row.get(index, classOf[Int]))
      case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.SMALLINT =>
        fromValue(SmallInt)(row.get(index, classOf[Short]))
      case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.TINYINT =>
        fromValue(TinyInt)(row.get(index, classOf[Byte]))
      case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.TIMESTAMP =>
        fromValue(Timestamp)(row.getInstant(index))
      case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.UUID =>
        fromValue(Uuid)(row.getUuid(index))
      case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.VARINT =>
        fromValue(VarInt)(row.getBigInteger(index))
      case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.TIMEUUID =>
        fromValue(TimeUuid)(row.getUuid(index))
      case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.VARCHAR =>
        fromValue(Text)(row.getString(index))
      case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.INET =>
        fromValue(Inet)(row.getInetAddress(index))
      case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.DATE =>
        fromValue(Date)(row.getLocalDate(index))
      case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.TIME =>
        fromValue(Time)(row.getLocalTime(index))
      case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.DURATION =>
        val dur = row.getCqlDuration(index)
        if (dur == null) {
          Null
        } else {
          fromValue(Duration)(dur.toString())
        }
      case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.LIST =>
        val list = row.getList[Any](index, classOf[Any])
        if (list == null) {
          Null
        } else {
          fromValue(List)(list.asScala.map(_.toString).toVector)
        }
      case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.MAP =>
        val map = row.getMap[String, Any](index, classOf[String], classOf[Any])
        if (map == null) {
          Null
        } else {
          fromValue(Map)(map.asScala.view.mapValues(_.toString).toMap)
        }
      case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.SET =>
        val set = row.getSet[Any](index, classOf[Any])
        if (set == null) {
          Null
        } else {
          fromValue(Set)(set.asScala.map(_.toString).toSet)
        }
      case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.TUPLE =>
        val tuple = row.getTupleValue(index)
        if (tuple == null) {
          Null
        } else {
          fromValue(Tuple)(tuple.getFormattedContents)
        }
      case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.UDT =>
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

  object DataType extends Enumeration {
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

    implicit val encoder: Encoder[DataType] = Encoder.enumEncoder(DataType)
    implicit val decoder: Decoder[DataType] = Decoder.enumDecoder(DataType)

    def fromCas(dt: CasDataType): DataType = {
      dt.getProtocolCode() match {
        case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.ASCII => Ascii
        case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.BIGINT => BigInt
        case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.BLOB => Blob
        case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.BOOLEAN => Bool
        case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.COUNTER => Counter
        case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.DECIMAL => Decimal
        case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.DOUBLE => Double
        case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.FLOAT => Float
        case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.INT => Integer
        case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.SMALLINT => SmallInt
        case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.TINYINT => TinyInt
        case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.TIMESTAMP => Timestamp
        case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.UUID => Uuid
        case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.VARINT => VarInt
        case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.TIMEUUID => TimeUuid
        case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.VARCHAR => Text
        case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.INET => Inet
        case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.DATE => Date
        case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.TIME => Time
        case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.DURATION => Duration
        case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.LIST => List
        case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.MAP => Map
        case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.SET => Set
        case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.TUPLE => Tuple
        case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.UDT => Udt
      }
    }
  }
  type DataType = DataType.Value
}

object ColumnValues {

  case object Null extends ColumnValue
  case class Unknown(v: String) extends ColumnValue

  case class Ascii(v: String) extends ColumnValue // ProtocolConstants.DataType.ASCII
  case class BigInt(v: Long) extends ColumnValue // ProtocolConstants.DataType.BIGINT
  case class Blob(v: String) extends ColumnValue // ProtocolConstants.DataType.BLOB
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
  case class List(v: Vector[String]) extends ColumnValue // ProtocolConstants.DataType.LIST
  case class Map(v: scala.collection.Map[String,String]) extends ColumnValue // ProtocolConstants.DataType.MAP
  case class Set(v: scala.collection.Set[String]) extends ColumnValue // ProtocolConstants.DataType.SET
  case class Tuple(v: String) extends ColumnValue // ProtocolConstants.DataType.TUPLE
  case class Udt(v: String) extends ColumnValue // ProtocolConstants.DataType.UDT
}


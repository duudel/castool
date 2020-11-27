package castool.cassandra

import cats.syntax.functor._
import io.circe.{Decoder, Encoder}
import io.circe.generic.auto._
import io.circe.generic.semiauto._
import io.circe.syntax._
import io.circe.HCursor

import com.datastax.oss.driver.api.core.cql
import java.util.Base64
import java.net.InetAddress

import scala.util.Try
import io.circe.DecodingFailure
import io.circe.Json

sealed trait ColumnValue

object ColumnValue {
  import ColumnValues._

  //implicit val inetDecoder: Decoder[InetAddress] = (cursor: HCursor) => for {
  //  s <- cursor.as[String]
  //  result <- Try(InetAddress.getByName(s))
  //    .toEither.left.map { ex => DecodingFailure(s"${ex.getMessage()}", cursor.history) }
  //} yield result

  //implicit val inetEncoder: Encoder[InetAddress] = (inet: InetAddress) => Json.fromString(inet.getHostAddress())

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
    /*List[Decoder[ColumnValue]](
    Decoder[Null].widen,
    Decoder[Ascii].widen,
    Decoder[BigInt].widen,
    Decoder[Blob].widen,
    //Decoder[BOOLEAN
    //Decoder[COUNTER
    Decoder[Decimal].widen,
    Decoder[Double].widen,
    Decoder[Float].widen,
    Decoder[Integer].widen,
    Decoder[Timestamp].widen,
    Decoder[Uuid].widen,
    Decoder[Varint].widen,
    Decoder[TimeUuid].widen,
    Decoder[Text].widen,
    Decoder[Unknown].widen,
  ).reduceLeft(_ or _)*/

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
          println(buf.array().mkString(","))
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
        fromValue(Double)(row.get(index, classOf[Double]))
      case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.FLOAT =>
        fromValue(Float)(row.get(index, classOf[Float]))
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

      case _ =>
        Unknown(columnDef.getName().asCql(true))
    }
  }
}

object ColumnValues {
  case object Null extends ColumnValue

  //public static final DataType ASCII = new PrimitiveType(ProtocolConstants.DataType.ASCII);
  case class Ascii(v: String) extends ColumnValue

  //public static final DataType BIGINT = new PrimitiveType(ProtocolConstants.DataType.BIGINT);
  case class BigInt(v: Long) extends ColumnValue

  //public static final DataType BLOB = new PrimitiveType(ProtocolConstants.DataType.BLOB);
  case class Blob(v: String) extends ColumnValue

  //public static final DataType BOOLEAN = new PrimitiveType(ProtocolConstants.DataType.BOOLEAN);
  case class Bool(v: Boolean) extends ColumnValue

  //public static final DataType COUNTER = new PrimitiveType(ProtocolConstants.DataType.COUNTER);
  case class Counter(v: Long) extends ColumnValue

  //public static final DataType DECIMAL = new PrimitiveType(ProtocolConstants.DataType.DECIMAL);
  case class Decimal(v: scala.BigDecimal) extends ColumnValue

  //public static final DataType DOUBLE = new PrimitiveType(ProtocolConstants.DataType.DOUBLE);
  case class Double(v: Double) extends ColumnValue
  
  //public static final DataType FLOAT = new PrimitiveType(ProtocolConstants.DataType.FLOAT);
  case class Float(v: Float) extends ColumnValue

  //public static final DataType INT = new PrimitiveType(ProtocolConstants.DataType.INT);
  case class Integer(v: Int) extends ColumnValue
  //public static final DataType SMALLINT = new PrimitiveType(ProtocolConstants.DataType.SMALLINT);
  case class SmallInt(v: Short) extends ColumnValue
  //public static final DataType TINYINT = new PrimitiveType(ProtocolConstants.DataType.TINYINT);
  case class TinyInt(v: Byte) extends ColumnValue
  
  //public static final DataType TIMESTAMP = new PrimitiveType(ProtocolConstants.DataType.TIMESTAMP);
  case class Timestamp(v: java.time.Instant) extends ColumnValue

  //public static final DataType UUID = new PrimitiveType(ProtocolConstants.DataType.UUID);
  case class Uuid(v: java.util.UUID) extends ColumnValue

  //public static final DataType VARINT = new PrimitiveType(ProtocolConstants.DataType.VARINT);
  case class VarInt(v: scala.BigInt) extends ColumnValue
  
  //public static final DataType TIMEUUID = new PrimitiveType(ProtocolConstants.DataType.TIMEUUID);
  case class TimeUuid(v: java.util.UUID) extends ColumnValue

  //public static final DataType TEXT = new PrimitiveType(ProtocolConstants.DataType.VARCHAR);
  case class Text(v: String) extends ColumnValue

  //public static final DataType INET = new PrimitiveType(ProtocolConstants.DataType.INET);
  case class Inet(v: InetAddress) extends ColumnValue
  
  //public static final DataType DATE = new PrimitiveType(ProtocolConstants.DataType.DATE);
  case class Date(v: java.time.LocalDate) extends ColumnValue
  
  //public static final DataType TIME = new PrimitiveType(ProtocolConstants.DataType.TIME);
  case class Time(v: java.time.LocalTime) extends ColumnValue

  case class Unknown(v: String) extends ColumnValue
/*
  public static final DataType DURATION = new PrimitiveType(ProtocolConstants.DataType.DURATION);
  */
}


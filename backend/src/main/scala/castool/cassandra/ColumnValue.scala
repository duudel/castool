package castool.cassandra

import cats.syntax.functor._
import io.circe.{Decoder, Encoder}
import io.circe.generic.auto._
//import io.circe.generic.semiauto._
import io.circe.syntax._

import com.datastax.oss.driver.api.core.cql

sealed trait ColumnValue

case object Null extends ColumnValue
case class Text(s: String) extends ColumnValue
case class Integer(i: Int) extends ColumnValue
case class Unknown(x: String) extends ColumnValue
/*
  public static final DataType ASCII = new PrimitiveType(ProtocolConstants.DataType.ASCII);
  public static final DataType BIGINT = new PrimitiveType(ProtocolConstants.DataType.BIGINT);
  public static final DataType BLOB = new PrimitiveType(ProtocolConstants.DataType.BLOB);
  public static final DataType BOOLEAN = new PrimitiveType(ProtocolConstants.DataType.BOOLEAN);
  public static final DataType COUNTER = new PrimitiveType(ProtocolConstants.DataType.COUNTER);
  public static final DataType DECIMAL = new PrimitiveType(ProtocolConstants.DataType.DECIMAL);
  public static final DataType DOUBLE = new PrimitiveType(ProtocolConstants.DataType.DOUBLE);
  public static final DataType FLOAT = new PrimitiveType(ProtocolConstants.DataType.FLOAT);
  public static final DataType INT = new PrimitiveType(ProtocolConstants.DataType.INT);
  public static final DataType TIMESTAMP = new PrimitiveType(ProtocolConstants.DataType.TIMESTAMP);
  public static final DataType UUID = new PrimitiveType(ProtocolConstants.DataType.UUID);
  public static final DataType VARINT = new PrimitiveType(ProtocolConstants.DataType.VARINT);
  public static final DataType TIMEUUID = new PrimitiveType(ProtocolConstants.DataType.TIMEUUID);
  public static final DataType INET = new PrimitiveType(ProtocolConstants.DataType.INET);
  public static final DataType DATE = new PrimitiveType(ProtocolConstants.DataType.DATE);
  public static final DataType TEXT = new PrimitiveType(ProtocolConstants.DataType.VARCHAR);
  public static final DataType TIME = new PrimitiveType(ProtocolConstants.DataType.TIME);
  public static final DataType SMALLINT = new PrimitiveType(ProtocolConstants.DataType.SMALLINT);
  public static final DataType TINYINT = new PrimitiveType(ProtocolConstants.DataType.TINYINT);
  public static final DataType DURATION = new PrimitiveType(ProtocolConstants.DataType.DURATION);
  */

object ColumnValue {
  implicit val decoder: Decoder[ColumnValue] = List[Decoder[ColumnValue]](
    Decoder[Text].widen,
    Decoder[Integer].widen,
    Decoder[Unknown].widen,
  ).reduceLeft(_ or _)

  def fromValue[C <: ColumnValue, T](f: T => C)(v: T): ColumnValue =
    Option(v).map(f).getOrElse(Null)

  def fromColumnDefinitionAndRow(columnDef: cql.ColumnDefinition, row: cql.Row, index: Int): ColumnValue = {
    columnDef.getType().getProtocolCode() match {
      case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.INT =>
        ColumnValue.fromValue(Integer)(row.getInt(index))
      case com.datastax.oss.protocol.internal.ProtocolConstants.DataType.VARCHAR =>
        ColumnValue.fromValue(Text)(row.getString(index))
      case _ =>
        Unknown(columnDef.getName().asCql(true))
    }
  }
}


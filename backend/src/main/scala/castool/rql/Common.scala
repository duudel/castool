package castool.rql

import scala.collection.SeqMap

object ValueType extends Enumeration {
  val Null: Value = Value
  val Num: Value = Value
  val Str: Value = Value
  val Bool: Value = Value
  val Obj: Value = Value
}

trait ValueTypeMapper[A <: Value] {
  def valueType: ValueType
}

object ResolveValueType {
  case class ValueTypeMapperImpl[A <: Value](valueType: ValueType) extends ValueTypeMapper[A]
  implicit val imp_null: ValueTypeMapper[Null] = ValueTypeMapperImpl[Null](ValueType.Null)
  implicit val imp_num: ValueTypeMapper[Num] = ValueTypeMapperImpl[Num](ValueType.Num)
  implicit val imp_str: ValueTypeMapper[Str] = ValueTypeMapperImpl[Str](ValueType.Str)
  implicit val imp_bool: ValueTypeMapper[Bool] = ValueTypeMapperImpl[Bool](ValueType.Bool)
  implicit val imp_obj: ValueTypeMapper[Obj] = ValueTypeMapperImpl[Obj](ValueType.Obj)

  implicit def valueType[A <: Value](implicit m: ValueTypeMapper[A]): ValueType = m.valueType
}

sealed trait Value

final case object Null extends Value
final case class Num(v: Double) extends Value
final case class Str(v: String) extends Value
final case class Bool(v: Boolean) extends Value
final case class Obj(v: Map[String, Value]) extends Value

case class Name private (n: String) extends AnyVal
object Name {
  def fromString(s: String): Either[String, Name] = {
    if (s.isEmpty) {
      Left("Empty string cannot be used as a name")
    } else if (!s.head.isLetter) {
      Left("First character of a name must be alphabetical")
    } else if (!s.forall(c => c.isLetterOrDigit || c == '_')) {
      Left("A name can contain only alpha numerical characters or '_'")
    } else {
      Right(Name(s))
    }
  }
  def fromStringUnsafe(s: String): Name = fromString(s).right.get

  implicit class NameInterpolator(sc: StringContext) {
    def n(args: Any*): Name = {
      val stringsIt = sc.parts.iterator
      val argsIt = args.iterator
      val builder = new java.lang.StringBuilder(stringsIt.next())
      while (stringsIt.hasNext) {
        builder.append(argsIt.next().toString())
        builder.append(stringsIt.next())
      }
      val s = builder.toString()
      Name.fromStringUnsafe(s)
    }
  }
}

case class InputRow(values: SeqMap[Name, Value])

object InputRow {
  def apply(iterable: Iterable[(Name, Value)]): InputRow = {
    val values = iterable.foldLeft(SeqMap.empty[Name, Value]) {
      case (acc, x) => acc + x
    }
    InputRow(values)
  }
  def apply(pairs: (Name, Value)*): InputRow = {
    InputRow(pairs)
  }
}

object Eval {
  type V = Value
  type Eval[+R <: V] = Seq[V] => R

  def arg[A<:V](args: Seq[V])(i: Int): A = args(i).asInstanceOf[A]

  def apply[R<:V](fn: Function0[R]): Eval[R] = args => fn()
  def apply[R<:V,A1<:V](fn: Function1[A1,R]): Eval[R] = args => fn(args(0).asInstanceOf[A1])
  def apply[R<:V,A1<:V,A2<:V](fn: Function2[A1,A2,R]): Eval[R] = args => fn(arg(args)(0), arg(args)(1))
  def apply[R<:V,A1<:V,A2<:V,A3<:V](fn: Function3[A1,A2,A3,R]): Eval[R] = args => fn(arg(args)(0), arg(args)(1), arg(args)(2))
  def apply[R<:V,A1<:V,A2<:V,A3<:V,A4<:V](fn: Function4[A1,A2,A3,A4,R]): Eval[R] = args => fn(arg(args)(0), arg(args)(1), arg(args)(2), arg(args)(3))
}

sealed trait Callable[+A <: Value] {
  def evaluate: Eval.Eval[A]
  def parameters: Seq[(String, ValueType)]
  def returnType: ValueType
}

case class FunctionDef[+A <: Value](
  evaluate: Eval.Eval[A],
  parameters: Seq[(String, ValueType)],
  returnType: ValueType,
) extends Callable[A]

case class AggregationDef[+A <: Value](
  evaluate: Eval.Eval[A],
  parameters: Seq[(String, ValueType)],
  returnType: ValueType,
  initialValue: Value,
  finalPhase: (Value, Num) => Value,
) extends Callable[A]

case class SourceDef(columns: SeqMap[Name, ValueType]) {
  def get(name: Name): Option[ValueType] = columns.get(name)
  def add(name: Name, valueType: ValueType): SourceDef = copy(columns = columns + (name -> valueType))
}

object SourceDef {
  def apply(iterable: Iterable[(Name, ValueType)]): SourceDef = {
    SourceDef(SeqMap(iterable.toSeq: _*))
  }
  def apply(pairs: (Name, ValueType)*): SourceDef = {
    SourceDef(SeqMap(pairs: _*))
  }
}

sealed trait Order
object Order {
  case object Asc extends Order
  case object Desc extends Order
}

sealed trait UnaryOp {
  def display: String
}
sealed abstract class UnOp(val display: String) extends UnaryOp
object UnaryOp {
  case object Not extends UnOp("!")
  case object Plus extends UnOp("+")
  case object Minus extends UnOp("-")
}

sealed trait BinaryOp {
  def display: String
}
object BinaryOp {
  sealed abstract class ArithmeticOp(val display: String) extends BinaryOp
  sealed abstract class CompareOp(val display: String) extends BinaryOp
  sealed abstract class StringOp(val display: String) extends BinaryOp
  sealed abstract class LogicalOp(val display: String) extends BinaryOp
  sealed abstract class AssignOp(val display: String) extends BinaryOp

  case object Plus extends ArithmeticOp("+")
  case object Minus extends ArithmeticOp("-")
  case object Multiply extends ArithmeticOp("*")
  case object Divide extends ArithmeticOp("/")
  case object Equal extends CompareOp("==")
  case object NotEqual extends CompareOp("!=")
  case object Less extends CompareOp("<")
  case object LessEq extends CompareOp("<=")
  case object Greater extends CompareOp(">")
  case object GreaterEq extends CompareOp(">=")
  case object Contains extends StringOp("contains")
  case object NotContains extends StringOp("!contains")
  case object And extends LogicalOp("and")
  case object Or extends LogicalOp("or")
  case object Assign extends AssignOp("=")
}

case class SourceLocation(line: Int, column: Int)

case class Location(pos: Int) {
  def atSourceText(text: String): SourceLocation = {
    if (pos > text.length()) throw new NoSuchElementException(s"Invalid position (pos=$pos) for given source text")
    var line = 1
    var column = 0
    var i = 0
    while (i < text.length && i < pos) {
      val ch = text.charAt(i)
      if (ch == '\n') {
        column = 0
        line += 1
      } else if (ch == '\r') {
      } else {
        column += 1
      }
      i += 1
    }
    SourceLocation(line, column)
  }
}



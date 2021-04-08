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
}

case class InputRow(values: Map[Name, Value])

object Eval {
  //type Eval[A] = Any
  def apply[A](fn: Function0[A]): Eval[A] = Eval[A](fn)
  def apply[A, P1](fn: Function1[P1, A]): Eval[A] = Eval[A](fn)
  def apply[A, P1, P2](fn: Function2[P1, P2, A]): Eval[A] = Eval[A](fn)
  def apply[A, P1, P2, P3](fn: Function3[P1, P2, P3, A]): Eval[A] = Eval[A](fn)
}
case class Eval[A](fn: Any) {
  def call(args: List[Any]): A = {
    args match {
      case Nil =>
        fn.asInstanceOf[Function0[A]].apply()
      case a1 :: Nil =>
        fn.asInstanceOf[Function1[Any, A]].apply(a1)
      case a1 :: a2 :: Nil =>
        fn.asInstanceOf[Function2[Any, Any, A]].apply(a1, a2)
      case a1 :: a2 :: a3 :: Nil =>
        fn.asInstanceOf[Function3[Any, Any, Any, A]].apply(a1, a2, a3)
    }
  }
}

sealed trait Callable[A <: Value] {
  def parameters: Map[String, ValueType]
  def returnType: ValueType
  def evaluate: Eval[A]
}

case class FunctionDef[A <: Value](
  evaluate: Eval[A],
  parameters: Map[String, ValueType],
  returnType: ValueType,
) extends Callable[A]

case class AggregationDef[A <: Value](
  evaluate: Eval[A],
  parameters: Map[String, ValueType],
  returnType: ValueType,
  initialValue: Value,
  finalPhase: (Value, Num) => Value,
) extends Callable[A]

case class SourceDef(columns: SeqMap[Name, ValueType]) {
  def get(name: Name): Option[ValueType] = columns.get(name)

  def project(names: Iterable[Name]): Either[Seq[String], SourceDef] = {
    val projected = names.map { name => name -> columns.get(name) }
    val errors = projected.collect {
      case (name, None) => s"No such column name as '${name.n}'"
    }
    if (errors.nonEmpty) {
      Left(errors.toSeq)
    } else {
      val result = projected.collect {
        case (name, Some(valueType)) => name -> valueType
      }
      Right(SourceDef(result))
    }
  }

  def extend(name: Name, valueType: ValueType): Either[Seq[String], SourceDef] = {
    // TODO: error if name is already defined
    val item: (Name, ValueType) = (name, valueType)
    val result = copy(columns = columns + item)
    Right(result)
  }
}

object SourceDef {
  def apply(iterable: Iterable[(Name, ValueType)]): SourceDef = {
    val columns = iterable.foldLeft(SeqMap.empty[Name, ValueType]) {
      case (acc, x) => acc + x
    }
    SourceDef(columns)
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


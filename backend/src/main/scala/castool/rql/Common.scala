package castool.rql
  
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


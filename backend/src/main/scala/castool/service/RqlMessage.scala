package castool.service

import io.circe._
import io.circe.generic.auto._
import io.circe.generic.semiauto._
import io.circe.syntax._

import castool.rql
import shapeless.Fin
import shapeless.Succ

sealed trait RqlMessage extends Product with Serializable

object RqlMessage {
  //implicit val valueDecoder: Decoder[rql.Value] = deriveDecoder
  //implicit val valueEncoder: Encoder[rql.Value] = deriveEncoder

  implicit val valueEncoder: Encoder[rql.Value] = Encoder.instance {
    case rql.Null => Json.Null
    case rql.Bool(v) if v => Json.True
    case rql.Bool(v) => Json.False
    case rql.Num(v) => Json.fromDoubleOrNull(v)
    case rql.Str(v) => Json.fromString(v)
    case rql.Obj(v) => rql.Obj(v).asJson
  }
  implicit val valueDecoder: Decoder[rql.Value] = deriveDecoder

  implicit val valueTypeDecoder: Decoder[rql.ValueType] = Decoder.decodeEnumeration(rql.ValueType)
  implicit val valueTypeEncoder: Encoder[rql.ValueType] = Encoder.encodeEnumeration(rql.ValueType)

  def encoderWithDiscriminator[A](fieldName: String)(toJson: A => (String, JsonObject)): Encoder[A] = Encoder.instance(toJson.andThen {
    case (name, js) =>
      val jsObj = js.add(fieldName, name.asJson)
      jsObj.asJson
  })

  def decoderWithDiscriminator[A](fieldName: String)(f: String => Decoder[_ <: A]) = Decoder.decodeJsonObject.flatMap {
    js => js(fieldName) match {
      case Some(fieldValue) =>
        fieldValue.asString match {
          case Some(discriminator) =>
            f(discriminator).map(x => x: A)
          case None =>
            Decoder.failed[A](DecodingFailure(s"Discriminator field '$fieldName' was not string", List.empty))
        }
      case None =>
        Decoder.failed[A](DecodingFailure(s"No discriminator field '$fieldName' found", List.empty))
    }
  }

  implicit val encoder: Encoder[RqlMessage] = encoderWithDiscriminator("_type") {
    case x: Success => ("Success", x.asJsonObject)
    case x: Error => ("Error", x.asJsonObject)
    case x: Rows => ("Rows", x.asJsonObject)
    case Finished => ("Finished", Finished.asJsonObject)
  }

  implicit val decoder: Decoder[RqlMessage] = decoderWithDiscriminator("_type") {
    case "Success" => deriveDecoder[Success]
    case "Error" => deriveDecoder[Error]
    case "Rows" => deriveDecoder[Rows]
    case "Finished" => deriveDecoder[Finished.type]
  }

  //implicit val encoder: Encoder[RqlMessage] = Encoder.instance {
  //  case x: Success => x.asJsonObject + ("_type" -> "Success")
  //  case x: Error => x.asJsonObject + ("_type" -> )
  //  case x: Rows => x.asJson
  //  case Finished => Finished.asJson
  //}

  case class Success(columns: Seq[(String, rql.ValueType)]) extends RqlMessage
  case class Error(error: String) extends RqlMessage
  case class Rows(rows: Seq[RqlService.ResultRow]) extends RqlMessage
  case object Finished extends RqlMessage
}



package com.spotify.ratatool.describe

import cats.kernel.Semigroup
import io.circe.{Encoder, Json}

case class Count(total: Long, nulled: Long) {
  val present: Long = total - nulled
  val isNullable: Boolean = nulled > 0
}

object Count {
  implicit val countSemigroup: Semigroup[Count] = new Semigroup[Count] {
    override def combine(x: Count, y: Count): Count = {
      Count(
        x.total + y.total,
        x.nulled + y.nulled
      )
    }
  }

  implicit val countEncoder: Encoder[Count] = new Encoder[Count] {

    import io.circe.syntax._

    override def apply(a: Count): Json = Json.obj(
      "total" -> a.total.asJson,
      "nullable" -> a.nulled.asJson,
      "present" -> a.present.asJson
    )
  }

  def fromOption[T](value: Option[T]): Count = {
    val nullable = if (value.isEmpty) 1 else 0
    Count(1, nullable)
  }
}

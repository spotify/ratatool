package com.spotify.ratatool.describe

import cats.kernel.Semigroup
import io.circe.{Encoder, Json}

case class MinMaxAvg(
  min: Long,
  max: Long,
  total: Long,
  count: Long
) {
  val mean: Option[Double] = if (count == 0) None else Some(total / count)
}

object MinMaxAvg {
  implicit val minMaxSemigroup: Semigroup[MinMaxAvg] = new Semigroup[MinMaxAvg] {
    override def combine(x: MinMaxAvg, y: MinMaxAvg): MinMaxAvg = {
      MinMaxAvg(
        min = math.min(x.min, y.min),
        max = math.max(x.max, y.max),
        total = x.total + y.total,
        count = x.count + y.count
      )
    }
  }

  implicit val encodeMinMaxAvg: Encoder[MinMaxAvg] = new Encoder[MinMaxAvg] {

    import io.circe.syntax._

    override def apply(a: MinMaxAvg): Json = Json.obj(
      "min" -> a.min.asJson,
      "max" -> a.max.asJson,
      "total" -> a.total.asJson,
      "count" -> a.count.asJson,
      "mean" -> a.mean.asJson
    )
  }

  val empty: MinMaxAvg = MinMaxAvg(Long.MinValue, Long.MaxValue, 0, 0)

  def fromLong(value: Option[Long]): MinMaxAvg = value match {
    case Some(x) =>
      MinMaxAvg(
        min = x,
        max = x,
        total = x,
        count = 1)
    case None =>
      MinMaxAvg.empty
  }
}

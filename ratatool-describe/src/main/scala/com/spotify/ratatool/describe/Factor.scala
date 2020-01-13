package com.spotify.ratatool.describe

import cats.kernel.Semigroup
import io.circe.{Encoder, Json}

sealed trait Factor[+X]

object Factor {
  val ReasonableThreshold: Int = 32

  class FactorSemigroup[X] extends Semigroup[Factor[X]] {
    override def combine(x: Factor[X], y: Factor[X]): Factor[X] = (x, y) match {
      case (x: Discrete[X], y: Discrete[X]) =>
        val values = x.values ++ y.values

        if (values.size > x.threshold || values.size > y.threshold) {
          Continuous
        } else {
          Discrete(
            values,
            math.min(x.threshold, y.threshold)
          )
        }
      case _ => Continuous
    }
  }

  class FactorEncoder[X: Encoder] extends Encoder[Factor[X]] {
    import io.circe.syntax._

    override def apply(a: Factor[X]): Json = a match {
      case Discrete(values, threshold) => Json.obj(
        "type" -> "discrete".asJson,
        "values" -> values.asJson,
        "threshold" -> threshold.asJson
      )

      case Continuous =>Json.obj(
        "type" -> "continuous".asJson
      )
    }
  }

  def fromValue[X](value: X, threshold: Int = ReasonableThreshold): Factor[X] = {
    Discrete(Set(value), threshold)
  }
}

case class Discrete[X](
  values: Set[X],
  threshold: Int
) extends Factor[X]

case object Continuous extends Factor[Nothing]
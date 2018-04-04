/*
 * Copyright 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.ratatool.diffy

import scala.collection.JavaConverters._
import scala.util.Try

/**
 * Delta type of a single node between two records.
 *
 * UNKNOWN - unknown type, no numeric delta is computed.
 * NUMERIC - numeric type, e.g. Long, Double, default delta is numeric difference.
 * STRING - string type, default delta is Levenshtein edit distance.
 * VECTOR - repeated numeric type, default delta is 1.0 - cosine similarity.
 */
object DeltaType extends Enumeration {
  val NUMERIC, STRING, VECTOR = Value
}

/** Delta value of a single node between two records. */
sealed trait DeltaValue

/** Delta value of unknown type. */
case object UnknownDelta extends DeltaValue {
  override def toString: String = "UNKNOWN"
}

/** Delta value with a known type and computed difference. */
case class TypedDelta(deltaType: DeltaType.Value, value: Double) extends DeltaValue {
  override def toString: String = s"$deltaType\t$value"
}

/** Companion objects for `TypedDelta`. */
object NumericDelta {
  def apply(value: Double): TypedDelta = TypedDelta(DeltaType.NUMERIC, value)
}
object StringDelta {
  def apply(value: Double): TypedDelta = TypedDelta(DeltaType.STRING, value)
}
object VectorDelta {
  def apply(value: Double): TypedDelta = TypedDelta(DeltaType.VECTOR, value)
}

/**
 * Delta of a single field between two records.
 *
 * @param field "." separated field identifier
 * @param left  left hand side value
 * @param right right hand side value
 * @param delta delta of numerical values
 */
case class Delta(field: String, left: Any, right: Any, delta: DeltaValue) {
  override def toString: String = s"$field\t$delta\t$left\t$right"
}

/**
 * Field level diff tool.
 *
 * Use `ignore` to specify set of fields to ignore during comparison.
 * Use `unordered` to specify set of fields to be treated as unordered, i.e. sort before comparison.
 */
abstract class Diffy[T](val ignore: Set[String],
                        val unordered: Set[String]) extends Serializable {

  def apply(x: T, y: T): Seq[Delta]

  /** Delta function for comparing a single node between two records. */
  def delta(x: Any, y: Any): DeltaValue = {
    val tryNum = Try(numericDelta(x.toString.toDouble, y.toString.toDouble))
    if (tryNum.isSuccess) {
      NumericDelta(tryNum.get)
    } else if (x.isInstanceOf[CharSequence] && y.isInstanceOf[CharSequence]) {
      StringDelta(stringDelta(x.toString, y.toString))
    } else {
      val tryVector = Try {
        val vx = x.asInstanceOf[java.util.List[_]].asScala.map(_.toString.toDouble)
        val vy = y.asInstanceOf[java.util.List[_]].asScala.map(_.toString.toDouble)
        vectorDelta(vx, vy)
      }
      if (tryVector.isSuccess) {
        VectorDelta(tryVector.get)
      } else {
        UnknownDelta
      }
    }
  }

  /** Distance function for numeric values, can be overridden by user. */
  def numericDelta(x: Double, y: Double): Double = y - x

  /** Distance function for string values, can be overridden by user. */
  def stringDelta(x: String, y: String): Double = Levenshtein.distance(x, y).toDouble

  /** Distance function for vector values, can be overridden by user. */
  def vectorDelta(x: Seq[Double], y: Seq[Double]): Double = CosineDistance.distance(x, y)

  /**
   * Sort a repeated field.
   *
   * Elements are sorted by `_.toString` since most types we deal with are not comparable.
   */
  def sortList(l: java.util.List[AnyRef]): java.util.List[AnyRef] =
    if (l == null) null else l.asScala.sortBy(_.toString).asJava

}

/**
 * Compute Levenshtein edit distance between two strings.
 * https://rosettacode.org/wiki/Levenshtein_distance#Scala
 */
object Levenshtein {
  def distance(s1: String, s2: String): Int = {
    val dist = Array.tabulate(s2.length + 1, s1.length + 1) { (j, i) =>
      if (j == 0) i else if (i == 0) j else 0
    }
    for (j <- 1 to s2.length; i <- 1 to s1.length) {
      dist(j)(i) = if (s2(j - 1) == s1(i - 1)) {
        dist(j - 1)(i - 1)
      } else {
        minimum(dist(j - 1)(i) + 1, dist(j)(i - 1) + 1, dist(j - 1)(i - 1) + 1)
      }
    }
    dist(s2.length)(s1.length)
  }
  private def minimum(i1: Int, i2: Int, i3: Int): Int = math.min(math.min(i1, i2), i3)
}

/**
 * Compute cosine distance between two vectors.
 */
object CosineDistance {
  def distance(x: Seq[Double], y: Seq[Double]): Double = 1.0 - sim(x, y)
  private def sim(x: Seq[Double], y: Seq[Double]): Double = {
    assert(x.size == y.size && x.nonEmpty)
    var dp = 0.0
    var xss = 0.0
    var yss = 0.0
    var i = 0
    while (i < x.size) {
      dp += x(i) * y(i)
      xss += x(i) * x(i)
      yss += y(i) * y(i)
      i += 1
    }
    dp / math.sqrt(xss * yss)
  }
}

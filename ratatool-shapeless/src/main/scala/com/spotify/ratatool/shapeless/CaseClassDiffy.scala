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

package com.spotify.ratatool.shapeless

import com.spotify.ratatool.Command
import com.spotify.ratatool.diffy.{BigDiffy, Delta, Diffy}
import com.spotify.ratatool.diffy.BigDiffy.diff
import com.spotify.scio.values.SCollection
import shapeless._
import shapeless.labelled.FieldType

import scala.reflect.ClassTag

sealed trait MapEncoder[A] {
  def toMap(in: A): Map[String, Any]
}

object MapEncoder {
  def apply[A](implicit encoder: MapEncoder[A]): MapEncoder[A] =
    encoder

  def createEncoder[A](func: A => Map[String, Any]): MapEncoder[A] = new MapEncoder[A] {
    override def toMap(in: A): Map[String, Any] = func(in)
  }

  implicit def stringEncoder[K <: Symbol](implicit witness: Witness.Aux[K]):
  MapEncoder[FieldType[K, String]] = {
    val name = witness.value.name
    createEncoder { v =>
      Map(name -> v.self)
    }
  }

  implicit def intEncoder[K <: Symbol](implicit witness: Witness.Aux[K]):
  MapEncoder[FieldType[K, Int]] = {
    val name = witness.value.name
    createEncoder { v =>
      Map(name -> v.self)
    }
  }

  implicit def longEncoder[K <: Symbol](implicit witness: Witness.Aux[K]):
  MapEncoder[FieldType[K, Long]] = {
    val name = witness.value.name
    createEncoder { v =>
      Map(name -> v.self)
    }
  }

  implicit def booleanEncoder[K <: Symbol](implicit witness: Witness.Aux[K]):
  MapEncoder[FieldType[K, Boolean]] = {
    val name = witness.value.name
    createEncoder { v =>
      Map(name -> v.self)
    }
  }

  implicit def floatEncoder[K <: Symbol](implicit witness: Witness.Aux[K]):
  MapEncoder[FieldType[K, Float]] = {
    val name = witness.value.name
    createEncoder { v =>
      Map(name -> v.self)
    }
  }

  implicit def doubleEncoder[K <: Symbol](implicit witness: Witness.Aux[K]):
  MapEncoder[FieldType[K, Double]] = {
    val name = witness.value.name
    createEncoder { v =>
      Map(name -> v.self)
    }
  }

  implicit val hnilEncoder: MapEncoder[HNil] =
    createEncoder[HNil](n => Map(HNil.toString -> HNil))

  implicit def seqEncoder[K <: Symbol, V](implicit witness: Witness.Aux[K]):
  MapEncoder[FieldType[K, Seq[V]]] = {
    val name = witness.value.name
    createEncoder { v =>
      Map(name -> v.toIndexedSeq)
    }
  }

  implicit def listEncoder[K <: Symbol, V](implicit witness: Witness.Aux[K]):
  MapEncoder[FieldType[K, List[V]]] = {
    val name = witness.value.name
    createEncoder { v =>
      Map(name -> v.toIndexedSeq)
    }
  }

  /*
  * null.asInstanceOf[V] derives the default value of primitive types
  * null.asInstanceOf[Int] = 0
  * null.asInstanceOf[Boolean] = false
  * null.asInstanceOf[String] = null
  * */
  implicit def optionEncoder[K <: Symbol, V](implicit witness: Witness.Aux[K]):
  MapEncoder[FieldType[K, Option[V]]] = {
    val name = witness.value.name
    createEncoder { v =>
      Map(name -> v.getOrElse(null.asInstanceOf[V]))
    }
  }

  //scalastyle:off
  implicit def hlistEncoder0[K <: Symbol, H, T <: HList](implicit hEncoder: Lazy[MapEncoder[FieldType[K, H]]],
                                                          tEncoder: Lazy[MapEncoder[T]]): MapEncoder[FieldType[K, H] :: T] = {
    createEncoder[FieldType[K, H] :: T](in => hEncoder.value.toMap(in.head) ++ tEncoder.value.toMap(in.tail))
  }

  implicit def hListEncoder1[K <: Symbol, H, T <: HList, R <: HList](implicit
                                                                      wit: Witness.Aux[K],
                                                                      gen: LabelledGeneric.Aux[H, R],
                                                                      encoderH: Lazy[MapEncoder[R]],
                                                                      encoderT: Lazy[MapEncoder[T]]
                                                                     ): MapEncoder[FieldType[K, H] :: T] =
    createEncoder(in =>
      encoderT.value.toMap(in.tail) ++ Map(wit.value.name -> encoderH.value.toMap(gen.to(in.head))))

  implicit def genericEncoder[A, R](implicit gen: LabelledGeneric.Aux[A, R],
                                    enc: MapEncoder[R]): MapEncoder[A] = {
    createEncoder(a => enc.toMap(gen.to(a)))
  }
}

class CaseClassDiffy[T](ignore: Set[String] = Set.empty,
                        unordered: Set[String] = Set.empty)(implicit diff: MapEncoder[T])
  extends Diffy[T](ignore, unordered) {
  import CaseClassDiffy._

  override def apply(x: T, y: T): Seq[Delta] = {
    diff(Some(asMap(x)), Some(asMap(y)))
      .filter(f => !hnilIgnore.exists(f.field.contains(_)))
  }

  private def hnilIgnore = ignore ++ Set(HNil.toString)

  private def diff(left: Any, right: Any, pref: String = ""): Seq[Delta] = {
    def diffMap(left: Map[String, Any], right: Map[String, Any], pref: String = pref) = {
      (left.keySet ++ right.keySet).toSeq
        .flatMap(k => diff(left.get(k), right.get(k), if (pref.isEmpty) k else s"$pref.$k"))
    }

    (left, right) match {
      case (Some(l: Map[String, Any]), Some(r: Map[String, Any])) => diffMap(l, r, pref)
      case (Some(l), Some(r)) => Seq(Delta(pref, l, r, delta(l, r)))
    }
  }
}

object CaseClassDiffy {
  private def asMap[T](in : T)(implicit diff: MapEncoder[T]) = diff.toMap(in)

  /** Diff two SCollection[T] **/
  def diffCaseClass[T : ClassTag : MapEncoder](lhs: SCollection[T],
                                               rhs: SCollection[T],
                                               keyFn: T => String,
                                               diffy: CaseClassDiffy[T]) : BigDiffy[T] =
    diff(lhs, rhs, diffy, keyFn)
}

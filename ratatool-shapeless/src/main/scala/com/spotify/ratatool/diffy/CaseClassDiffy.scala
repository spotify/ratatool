/*
 * Copyright 2018 Spotify AB.
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

import shapeless._
import shapeless.labelled._
import shapeless.ops.hlist._
import shapeless.syntax.std.tuple._

import scala.language.experimental.macros

object MapTuple extends Poly1 {
  //scalastyle:off line.size.limit
  implicit def isDiff[K, A](implicit wk: Witness.Aux[K]): MapTuple.Case.Aux[(FieldType[K, A], FieldType[K, A]), (K, (A, A))] =
    at[(FieldType[K, A], FieldType[K, A])] {
      case (x, y) => (wk.value, (x, y))
    }
  //scalastyle:on
}

//class CaseClassDiffy[T, R <: HList, Z <: HList, L](gen: LabelledGeneric.Aux[T, R],
//                                    ignore: Set[String] = Set.empty,
//                                    unordered: Set[String] = Set.empty)
//                                   (implicit zip: Zip.Aux[R :: R :: HNil, Z],
//                                    toTraversable: ToTraversable.Aux[Z, List, L])
//  extends Diffy[T](ignore, unordered) {
//  override def apply(x: T, y: T): Seq[Delta] = {
//    val xL = gen.to(x)
//    val yL = gen.to(y)
//    diff(xL, yL)
//  }
//
//  def diff(x: R, y: R): Seq[Delta] = {
//    val a = x.zip(y).toList
//    Seq()
//  }
//}

private[diffy] object ClassToList {
  def apply[T, R <: HList, Z <: HList, M <: HList, L](gen: LabelledGeneric.Aux[T, R], x: T, y: T)
                                                     (
                                                       implicit zip: Zip.Aux[R :: R :: HNil, Z],
                                                       map: Mapper.Aux[MapTuple.type, Z, M],
                                                       toList: ToTraversable.Aux[M, List, L]
                                                     ): Seq[Any] = {
    gen.to(x).zip(gen.to(y)).map(MapTuple).toList
  }
}



class CaseClassDiffy[T, R <: HList, Z <: HList, M <: HList, L](gen: LabelledGeneric.Aux[T, R],
                                                  ignore: Set[String] = Set.empty,
                                                  unordered: Set[String] = Set.empty)
                                                (m: Map[String, LabelledGeneric.Aux[_, _]] = Map(),
                                                 prefix: String = "")
                                                  (implicit zip: Zip.Aux[R :: R :: HNil, Z],
                                                    map: Mapper.Aux[MapTuple.type, Z, M],
                                                    toList: ToTraversable.Aux[M, List, L])
  extends Diffy[T](ignore, unordered) {
  import reflect.runtime.universe._

  override def apply(x: T, y: T): Seq[Delta] = {
    diff(x, y)
  }

  def diff(x: T, y: T): Seq[Delta] = {
    val transformed: Seq[Any] = ClassToList(gen, x, y)
    transformed.flatMap {
      case (f: String, (a, b)) =>
        val symbol = runtimeMirror(a.getClass.getClassLoader).reflect(a).symbol
        val field = s"$prefix.$f"
        if (ignore.contains(f)) {
          Nil
        }
        else if (symbol.isCaseClass) {
          new CaseClassDiffy(m(a.getClass.toString), ignore, unordered)(m, field).apply(x, y)
        }
        else if (a.isInstanceOf[Seq[_]] && unordered.contains(f)) {
          val aSort = a.asInstanceOf[Seq[_]].sortBy(_.toString)
          val bSort = b.asInstanceOf[Seq[_]].sortBy(_.toString)
          if (aSort == bSort) Nil else Seq(Delta(field, aSort, bSort, delta(aSort, bSort)))
        }
        else {
          if (a == b) Nil else Seq(Delta(field, a, b, delta(a, b)))
        }
      case a: Any =>
        throw new Exception(s"Must be a tuple of the form (field, (value1, value2)). ${a}")
    }
  }
}

/**
 * class CaseClassDiffy[T, R <: HList, Z <: HList, M <: HList, L](gen: LabelledGeneric.Aux[T, R],
                                                          ignore: Set[String] = Set.empty,
                                                          unordered: Set[String] = Set.empty)
                                                         (implicit zip: Zip.Aux[R :: R :: HNil, Z],
                                                          map: Mapper.Aux[IsDiff.type, Z, M],
                                                          toList: ToTraversable.Aux[M, List, L])
  extends Diffy[T](ignore, unordered) {
  override def apply(x: T, y: T): Seq[Delta] = {
    val xL = gen.to(x)
    val yL = gen.to(y)
    diff(xL, yL)
  }

  def diff(x: R, y: R): Seq[Delta] = {
    val transformed: Seq[Any] = x.zip(y).map(IsDiff).toList
    transformed.map{
      case (f: String, (a, b)) =>
        if (a == b) {
          Nil
        }
        else {
          if (a.isInstanceOf[Seq] && unordered.contains(f)) {
            val aSort = a.asInstanceOf[Seq[_]].sortBy(_.toString)
            val bSort = b.asInstanceOf[Seq[_]].sortBy(_.toString)
            if (aSort == bSort) Nil else Seq(Delta(f, aSort, bSort, delta(aSort, bSort)))
          }
          else {
            if (a == b) Nil else Seq(Delta(f, a, b, delta(a, b)))
          }
        }
      case _ => throw new Exception("Must be a tuple of (field, (value1, value2))")
    }
  }
}
 */


//
//class CaseClassDiffy[T](ignore: Set[String] = Set.empty, unordered: Set[String] = Set.empty)
//                       (implicit gen: LabelledGeneric.Aux[T, _ <: HList])
//  extends Diffy[T](ignore, unordered) {
//  override def apply(x: T, y: T): Seq[Delta] = {
//    Seq()
//  }
//
//  def diff[R <: HList](x: R, y: R)(implicit zipper: Zip[R :: R :: HNil]): Seq[Delta] = {
//    val a = x.zip(y)(zipper)
//    Seq()
//  }
//}

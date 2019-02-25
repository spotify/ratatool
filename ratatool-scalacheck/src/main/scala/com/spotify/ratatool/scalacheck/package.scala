/*
 * Copyright 2017 Spotify AB.
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

package com.spotify.ratatool

import com.google.api.services.bigquery.model.TableRow
import com.google.protobuf.Message
import org.apache.avro.specific.SpecificRecord
import org.scalacheck.Gen

import scala.util.Try
import scala.language.implicitConversions

package object scalacheck extends AvroGeneratorOps
  with ProtoBufGeneratorOps
  with TableRowGeneratorOps {

  implicit class TupGen[A, B](tup: (Gen[A], Gen[B])) {
    def tupled: Gen[(A, B)] = {
      val (l, r) = tup
      l.flatMap(a => r.map(b => (a, b)))
    }
  }

  implicit class RichAvroGen[T <: SpecificRecord](gen: Gen[T]) {
    def amend[U](g: Gen[U])(fns: (T => (U => Unit))*): Gen[T] = {
      for (r <- gen; v <- g) yield {
        fns.foreach(f => f(r)(v))
        r
      }
    }

    def tryAmend[U](g: Gen[U])(f: T => (U => Unit)): Gen[T] = {
      for (r <- gen; v <- g) yield {
        Try(f(r)(v))
        r
      }
    }
  }

  implicit def scalaIntSetter(fn: (java.lang.Integer => Unit)): Int => Unit =
    fn.asInstanceOf[Int => Unit]

  implicit def scalaLongSetter(fn: (java.lang.Long => Unit)): Long => Unit =
    fn.asInstanceOf[Long => Unit]

  implicit def scalaFloatSetter(fn: (java.lang.Float => Unit)): Float => Unit =
    fn.asInstanceOf[Float => Unit]

  implicit def scalaDoubleSetter(fn: (java.lang.Double => Unit)): Double => Unit =
    fn.asInstanceOf[Double => Unit]

  implicit def scalaBooleanSetter(fn: (java.lang.Boolean => Unit)): Boolean => Unit =
    fn.asInstanceOf[Boolean => Unit]

  implicit def scalaStringSetter(fn: (java.lang.CharSequence => Unit)): String => Unit =
    fn.asInstanceOf[String => Unit]

  private type Record = java.util.Map[String, AnyRef]

  implicit class RichTableRowGen(gen: Gen[TableRow]) {
    def amend[U](g: Gen[U])(fns: (TableRow => (AnyRef => Record))*): Gen[TableRow] = {
      for (r <- gen; v <- g) yield {
        fns.foreach(f => f(r)(v.asInstanceOf[AnyRef]))
        r
      }
    }

    def tryAmend[U](g: Gen[U])(f: TableRow => (AnyRef => Record)): Gen[TableRow] = {
      for (r <- gen; v <- g) yield {
        Try(f(r)(v.asInstanceOf[AnyRef]))
        r
      }
    }
  }

  implicit class RichTableRow(r: Record) {
    def getRecord(name: AnyRef): Record = {
      r.get(name).asInstanceOf[Record]
    }
    def set(fieldName: String): AnyRef => Record = { v =>
      r.put(fieldName, v)
      r
    }
  }

  implicit class RichProtobufBuilder[T <: Message.Builder](gen: Gen[T]) {
    def amend[U](g: Gen[U])(fns: (T => (U => T))*): Gen[T] = {
      for (r <- gen; v <- g) yield {
        fns.foldLeft(r)((r, f) => f(r)(v))
      }
    }

    def tryAmend[U](g: Gen[U])(f: T => (U => T)): Gen[T] = {
      for (r <- gen; v <- g) yield {
        Try(f(r)(v)).getOrElse(r)
      }
    }
  }

  implicit class RichAvroTupGen[A <: SpecificRecord, B <: SpecificRecord](gen: Gen[(A, B)]) {
    def amend2[U](a: Gen[U])(f: A => (U => Unit), g: B => (U => Unit)): Gen[(A, B)] = {
      for ((r1, r2) <- gen; v <- a) yield {
        f(r1)(v)
        g(r2)(v)
        (r1, r2)
      }
    }

    def tryAmend2[U](a: Gen[U])(f: A => (U => Unit), g: B => (U => Unit)): Gen[(A, B)] = {
      for ((r1, r2) <- gen; v <- a) yield {
        Try(f(r1)(v))
        Try(g(r2)(v))
        (r1, r2)
      }
    }
  }

  implicit class RichTableRowTupGen(gen: Gen[(TableRow, TableRow)]) {
    def amend2[U](a: Gen[U])(f: TableRow => (AnyRef => Record),
                            g: TableRow => (AnyRef => Record)): Gen[(TableRow, TableRow)] = {
      for ((r1, r2) <- gen; v <- a) yield {
        f(r1)(v.asInstanceOf[AnyRef])
        g(r2)(v.asInstanceOf[AnyRef])
        (r1, r2)
      }
    }

    def tryAmend2[U](a: Gen[U])(f: TableRow => (AnyRef => Record),
                               g: TableRow => (AnyRef => Record)): Gen[(TableRow, TableRow)] = {
      for ((r1, r2) <- gen; v <- a) yield {
        Try(f(r1)(v.asInstanceOf[AnyRef]))
        Try(g(r2)(v.asInstanceOf[AnyRef]))
        (r1, r2)
      }
    }
  }

  implicit class RichProtobufBuilderTup[A <: Message.Builder, B <: Message.Builder]
  (gen: Gen[(A, B)]) {
    def amend2[U](a: Gen[U])(f: A => (U => A), g: B => (U => B)): Gen[(A, B)] = {
      for ((r1, r2) <- gen; v <- a) yield {
        (f(r1)(v), g(r2)(v))
      }
    }

    def tryAmend2[U](a: Gen[U])(f: A => (U => A), g: B => (U => B)): Gen[(A, B)] = {
      for ((r1, r2) <- gen; v <- a) yield {
        (Try(f(r1)(v)).getOrElse(r1), Try(g(r2)(v)).getOrElse(r2))
      }
    }
  }

}

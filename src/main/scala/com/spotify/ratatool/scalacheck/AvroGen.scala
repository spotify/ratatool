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

package com.spotify.ratatool.scalacheck

import com.spotify.ratatool.generators.AvroGenerator
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecord
import org.scalacheck._

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.Try

object AvroGen {

  /** ScalaCheck generator of Avro generic records. */
  def avroOf(schema: Schema): Gen[GenericRecord] =
    Gen.const(0).map(_ => AvroGenerator.avroOf(schema))

  /** ScalaCheck generator of Avro specific records. */
  def avroOf[T <: SpecificRecord : ClassTag]: Gen[T] =
    Gen.const(0).map(_ => AvroGenerator.avroOf[T])

  implicit class RichAvroGen[T <: SpecificRecord](gen: Gen[T]) {

    def amend[U](g: Gen[U])(f: T => (U => Unit)): Gen[T] = {
      for (r <- gen; v <- g) yield {
        f(r)(v)
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

}

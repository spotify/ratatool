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

import com.google.cloud.dataflow.sdk.coders.AvroCoder
import com.google.cloud.dataflow.sdk.util.CoderUtils
import com.spotify.ratatool.avro.specific.{NullableNestedRecord, TestRecord}
import com.spotify.ratatool.generators.AvroGenerator
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class AvroDiffyTest extends FlatSpec with Matchers {

  def jl[T](x: T*): java.util.List[T] = List(x: _*).asJava

  "AvroDiffy" should "support primitive fields" in {
    val x = NullableNestedRecord.newBuilder().setIntField(10).setLongField(20L).build()
    val y = NullableNestedRecord.newBuilder().setIntField(10).setLongField(20L).build()
    val z = NullableNestedRecord.newBuilder().setIntField(10).setLongField(200L).build()

    AvroDiffy(x, y) should equal (Nil)
    AvroDiffy(x, z) should equal (Seq(Delta("long_field", 20L, 200L)))
  }

  it should "support nested fields" in {
    val coder = AvroCoder.of(classOf[TestRecord])

    val x = AvroGenerator.avroOf[TestRecord]
    x.getNullableFields.setIntField(10)
    x.getNullableFields.setLongField(20L)
    x.getNullableFields.setStringField("hello")

    val y = CoderUtils.clone(coder, x)
    val z = CoderUtils.clone(coder, x)
    z.getNullableFields.setLongField(200L)
    z.getNullableFields.setStringField("world")

    AvroDiffy(x, y) should equal (Nil)
    AvroDiffy(x, z) should equal (Seq(
      Delta("nullable_fields.long_field", 20L, 200L),
      Delta("nullable_fields.string_field", "hello", "world")))
  }

  it should "support repeated fields" in {
    val coder = AvroCoder.of(classOf[TestRecord])

    val x = AvroGenerator.avroOf[TestRecord]
    x.getRepeatedFields.setIntField(jl(10, 11))
    x.getRepeatedFields.setLongField(jl(20L, 21L))
    x.getRepeatedFields.setStringField(jl("hello", "world"))

    val y = CoderUtils.clone(coder, x)
    val z = CoderUtils.clone(coder, x)
    z.getRepeatedFields.setLongField(jl(20L, 210L))
    z.getRepeatedFields.setStringField(jl("Hello", "World"))

    AvroDiffy(x, y) should equal (Nil)
    AvroDiffy(x, z) should equal (Seq(
      Delta("repeated_fields.long_field", jl(20L, 21L), jl(20L, 210L)),
      Delta("repeated_fields.string_field", jl("hello", "world"), jl("Hello", "World"))))
  }

}

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
    AvroDiffy(x, z) should equal (Seq(Delta("long_field", 20L, 200L, Some(180.0))))
  }

  it should "support nested fields" in {
    val coder = AvroCoder.of(classOf[TestRecord])

    val nnr = AvroGenerator.avroOf[NullableNestedRecord]
    nnr.setIntField(10)
    nnr.setLongField(20L)
    nnr.setStringField("hello")

    val x = AvroGenerator.avroOf[TestRecord]
    x.setNullableNestedField(nnr)

    val y = CoderUtils.clone(coder, x)
    val z1 = CoderUtils.clone(coder, x)
    z1.getNullableNestedField.setLongField(200L)
    z1.getNullableNestedField.setStringField("world")
    val z2 = CoderUtils.clone(coder, x)
    z2.setNullableNestedField(null)
    val z3 = CoderUtils.clone(coder, z2)

    AvroDiffy(x, y) should equal (Nil)
    AvroDiffy(x, z1) should equal (Seq(
      Delta("nullable_nested_field.long_field", 20L, 200L, Some(180.0)),
      Delta("nullable_nested_field.string_field", "hello", "world", None)))
    AvroDiffy(x, z2) should equal (Seq(
      Delta("nullable_nested_field", nnr, null, None)))
    AvroDiffy(z2, z3) should equal (Nil)
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
      Delta("repeated_fields.long_field", jl(20L, 21L), jl(20L, 210L), None),
      Delta("repeated_fields.string_field", jl("hello", "world"), jl("Hello", "World"), None)))
  }

}

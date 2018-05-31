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

import com.spotify.ratatool.avro.specific._
import com.spotify.ratatool.scalacheck._
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.coders.AvroCoder
import org.apache.beam.sdk.util.CoderUtils
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class AvroDiffyTest extends FlatSpec with Matchers {

  def jl[T](x: T*): java.util.List[T] = List(x: _*).asJava

  val d = new AvroDiffy[GenericRecord]()

  "AvroDiffy" should "support primitive fields" in {
    val x = NullableNestedRecord.newBuilder().setIntField(10).setLongField(20L).build()
    val y = NullableNestedRecord.newBuilder().setIntField(10).setLongField(20L).build()
    val z = NullableNestedRecord.newBuilder().setIntField(10).setLongField(200L).build()

    d(x, y) should equal (Nil)
    d(x, z) should equal (Seq(
      Delta("long_field", 20L, 200L, NumericDelta(180.0))))
  }

  it should "support nested fields" in {
    val coder = AvroCoder.of(classOf[TestRecord])

    val nnr = specificRecordOf[NullableNestedRecord].sample.get
    nnr.setIntField(10)
    nnr.setLongField(20L)
    nnr.setStringField("hello")

    val x = specificRecordOf[TestRecord].sample.get
    x.setNullableNestedField(nnr)

    val y = CoderUtils.clone(coder, x)
    val z1 = CoderUtils.clone(coder, x)
    z1.getNullableNestedField.setLongField(200L)
    z1.getNullableNestedField.setStringField("Hello")
    val z2 = CoderUtils.clone(coder, x)
    z2.setNullableNestedField(null)
    val z3 = CoderUtils.clone(coder, z2)

    d(x, y) should equal (Nil)
    d(x, z1) should equal (Seq(
      Delta("nullable_nested_field.long_field", 20L, 200L, NumericDelta(180.0)),
      Delta("nullable_nested_field.string_field", "hello", "Hello", StringDelta(1.0))))
    d(x, z2) should equal (Seq(
      Delta("nullable_nested_field", nnr, null, UnknownDelta)))
    d(z2, z3) should equal (Nil)
  }

  it should "support repeated fields" in {
    val coder = AvroCoder.of(classOf[TestRecord])


    val x = specificRecordOf[TestRecord].sample.get
    x.getRepeatedFields.setIntField(jl(10, 11))
    x.getRepeatedFields.setLongField(jl(20L, 21L))
    x.getRepeatedFields.setStringField(jl("hello", "world"))

    val y = CoderUtils.clone(coder, x)
    val z = CoderUtils.clone(coder, x)
    z.getRepeatedFields.setLongField(jl(-20L, -21L))
    z.getRepeatedFields.setStringField(jl("Hello", "World"))

    d(x, y) should equal (Nil)
    d(x, z) should equal (Seq(
      Delta("repeated_fields.long_field",
        jl(20L, 21L), jl(-20L, -21L), VectorDelta(2.0)),
      Delta("repeated_fields.string_field",
        jl("hello", "world"), jl("Hello", "World"), UnknownDelta)))
  }

  it should "support ignore" in {
    val x = NullableNestedRecord.newBuilder().setIntField(10).setLongField(20L).build()
    val y = NullableNestedRecord.newBuilder().setIntField(10).setLongField(20L).build()
    val z = NullableNestedRecord.newBuilder().setIntField(20).setLongField(200L).build()

    val di = new AvroDiffy[GenericRecord](Set("int_field"))
    di(x, y) should equal (Nil)
    di(x, z) should equal (Seq(
      Delta("long_field", 20L, 200L, NumericDelta(180.0))))
  }

  it should "support unordered" in {
    val coder = AvroCoder.of(classOf[TestRecord])

    val a = NullableNestedRecord.newBuilder().setIntField(10).setLongField(100L).build()
    val b = NullableNestedRecord.newBuilder().setIntField(20).setLongField(200L).build()
    val c = NullableNestedRecord.newBuilder().setIntField(30).setLongField(300L).build()

    val x = specificRecordOf[TestRecord].sample.get
    x.setRepeatedNestedField(jl(a, b, c))
    val y = CoderUtils.clone(coder, x)
    val z = CoderUtils.clone(coder, x)
    z.setRepeatedNestedField(jl(a, c, b))

    val du = new AvroDiffy[GenericRecord](unordered = Set("repeated_nested_field"))
    du(x, y) should equal (Nil)
    du(x, z) should equal (Nil)
    d(x, z) should equal (Seq(
      Delta("repeated_nested_field", jl(a, b, c), jl(a, c, b), UnknownDelta)))
  }

  it should "support unordered nested" in {
    val drnrCoder = AvroCoder.of(classOf[RepeatedRecord])
    val drrCoder = AvroCoder.of(classOf[DeeplyRepeatedRecord])

    val a = avroOf[RepeatedRecord].sample.get
    a.setNestedRepeatedField(jl(10, 20, 30))
    val b = CoderUtils.clone(drnrCoder, a)
    b.setNestedRepeatedField(jl(10, 20, 30))
    val c = CoderUtils.clone(drnrCoder, a)
    c.setNestedRepeatedField(jl(10, 30, 20))

    val x = avroOf[DeeplyRepeatedRecord].sample.get
    x.setRepeatedRecord(jl(a, b, c))
    val y = CoderUtils.clone(drrCoder, x)
    y.setRepeatedRecord(jl(a, b, c))
    val z = CoderUtils.clone(drrCoder, x)
    z.setRepeatedRecord(jl(a, c, b))

    val du = new AvroDiffy[DeeplyRepeatedRecord](
      unordered = Set("repeated_record", "repeated_nested_field.nested_repeated_field"),
      unorderedFieldKeys = Map("repeated_record" -> "string_field"))

    du(x, y) should equal (Nil)
    du(x, z) should equal (Nil)
    d(x, z) should equal (Seq(
      Delta(
        "repeated_record",
        jl(a, b, c),
        jl(a, c, b),
        UnknownDelta)))
  }
}

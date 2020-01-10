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

import com.spotify.ratatool.scalacheck._
import com.spotify.ratatool.proto.Schemas._
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class ProtoBufDiffyTest extends FlatSpec with Matchers {

  def jl[T](x: T*): java.util.List[T] = List(x: _*).asJava

  "ProtoBufDiffy" should "support primitive fields" in {
    val x = OptionalNestedRecord.newBuilder().setInt32Field(10).setInt64Field(20L).build()
    val y = OptionalNestedRecord.newBuilder().setInt32Field(10).setInt64Field(20L).build()
    val z = OptionalNestedRecord.newBuilder().setInt32Field(10).setInt64Field(200L).build()

    val d = new ProtoBufDiffy[OptionalNestedRecord]()
    d(x, y) should equal (Nil)
    d(x, z) should equal (Seq(
      Delta("int64_field", Option(20L), Option(200L), NumericDelta(180.0))))
  }

  it should "support nested fields" in {
    val base = protoBufOf[TestRecord].sample.get

    val onr = protoBufOf[OptionalNestedRecord].sample.get.toBuilder
      .setInt32Field(10)
      .setInt64Field(20L)
      .setStringField("hello")
      .build()

    val x = TestRecord.newBuilder(base)
      .setOptionalNestedField(onr)
      .build()
    val y = TestRecord.newBuilder(x).build()
    val z1 = TestRecord.newBuilder(x)
      .setOptionalNestedField(
        OptionalNestedRecord.newBuilder(onr)
          .setInt64Field(200L)
          .setStringField("Hello")
      ).build()
    val z2 = TestRecord.newBuilder(x).clearOptionalNestedField().build()
    val z3 = TestRecord.newBuilder(x).clearOptionalNestedField().build()

    val d = new ProtoBufDiffy[TestRecord]()
    d(x, y) should equal (Nil)
    d(x, z1) should equal (Seq(
      Delta("optional_nested_field.int64_field", Option(20L), Option(200L), NumericDelta(180.0)),
      Delta("optional_nested_field.string_field", Option("hello"), Option("Hello"),
        StringDelta(1.0))))
    d(x, z2) should equal (Seq(
      Delta("optional_nested_field", Option(onr), None, UnknownDelta)))
    d(z2, z3) should equal (Nil)
  }

  it should "support repeated fields" in {
    val base = protoBufOf[TestRecord].sample.get
    val x = TestRecord.newBuilder(base)
      .setRepeatedFields(
        RepeatedNestedRecord.newBuilder(base.getRepeatedFields)
          .clearInt32Field()
          .clearInt64Field()
          .clearStringField()
          .addAllInt32Field(jl(10, 11))
          .addAllInt64Field(jl(20L, 21L))
          .addAllStringField(jl("hello", "world"))
          .build()
      ).build()
    val y = TestRecord.newBuilder(x).build()
    val z = TestRecord.newBuilder(x)
      .setRepeatedFields(
        RepeatedNestedRecord.newBuilder(x.getRepeatedFields)
          .clearInt64Field()
          .clearStringField()
          .addAllInt64Field(jl(-20L, -21L))
          .addAllStringField(jl("Hello", "World"))
          .build()
      ).build()

    val d = new ProtoBufDiffy[TestRecord]()
    d(x, y) should equal (Nil)
    d(x, z) should equal (Seq(
      Delta("repeated_fields.int64_field", Option(jl(20L, 21L)), Option(jl(-20L, -21L)),
        VectorDelta(2.0)),
      Delta("repeated_fields.string_field", Option(jl("hello", "world")),
        Option(jl("Hello", "World")), UnknownDelta)))
  }

  it should "support ignore" in {
    val x = OptionalNestedRecord.newBuilder().setInt32Field(10).setInt64Field(20L).build()
    val y = OptionalNestedRecord.newBuilder().setInt32Field(10).setInt64Field(20L).build()
    val z = OptionalNestedRecord.newBuilder().setInt32Field(20).setInt64Field(200L).build()

    val di = new ProtoBufDiffy[OptionalNestedRecord](Set("int32_field"))
    di(x, y) should equal (Nil)
    di(x, z) should equal (Seq(
      Delta("int64_field", Option(20L), Option(200L), NumericDelta(180.0))))
  }

  it should "support unordered" in {
    val a = OptionalNestedRecord.newBuilder().setInt32Field(10).setInt64Field(100L).build()
    val b = OptionalNestedRecord.newBuilder().setInt32Field(20).setInt64Field(200L).build()
    val c = OptionalNestedRecord.newBuilder().setInt32Field(30).setInt64Field(300L).build()

    val base = protoBufOf[TestRecord].sample.get
    val x = TestRecord.newBuilder(base)
      .clearRepeatedNestedField()
      .addAllRepeatedNestedField(jl(a, b, c))
      .build()
    val y = TestRecord.newBuilder(base)
      .clearRepeatedNestedField()
      .addAllRepeatedNestedField(jl(a, b, c))
      .build()
    val z = TestRecord.newBuilder(base)
      .clearRepeatedNestedField()
      .addAllRepeatedNestedField(jl(a, c, b))
      .build()

    val du = new ProtoBufDiffy[TestRecord](unordered = Set("repeated_nested_field"))
    du(x, y) should equal (Nil)
    du(x, z) should equal (Nil)
    val d = new ProtoBufDiffy[TestRecord]
    d(x, z) should equal (Seq(
      Delta("repeated_nested_field", Option(jl(a, b, c)), Option(jl(a, c, b)), UnknownDelta)))
  }

  it should "support unordered nested" in {
    val a = RepeatedRecord.newBuilder().setStringField("hello")
      .clearNestedRepeatedField().addAllNestedRepeatedField(jl(10, 20, 30)).build
    val b = RepeatedRecord.newBuilder().setStringField("world")
      .clearNestedRepeatedField().addAllNestedRepeatedField(jl(10, 20, 30)).build
    val c = RepeatedRecord.newBuilder().setStringField("!")
      .clearNestedRepeatedField().addAllNestedRepeatedField(jl(10, 30, 20)).build

    val x = DeeplyRepeatedRecord.newBuilder().clearRepeatedRecord()
      .addAllRepeatedRecord(jl(a, b, c)).build
    val y = DeeplyRepeatedRecord.newBuilder().clearRepeatedRecord()
      .addAllRepeatedRecord(jl(a, b, c)).build
    val z = DeeplyRepeatedRecord.newBuilder().clearRepeatedRecord()
      .addAllRepeatedRecord(jl(a, c, b)).build

    val du = new ProtoBufDiffy[DeeplyRepeatedRecord](
      unordered = Set("repeated_record", "repeated_record.nested_repeated_field"),
      unorderedFieldKeys = Map("repeated_record" -> "string_field"))
    val d = new ProtoBufDiffy[DeeplyRepeatedRecord]()

    du(x, y) should equal (Nil)
    du(x, z) should equal (Nil)
    d(x, z) should equal (Seq(
      Delta("repeated_record", Option(jl(a, b, c)), Option(jl(a, c, b)), UnknownDelta)))
  }

  it should "support unordered nested of different lengths" in {
    val a = RepeatedRecord.newBuilder().setStringField("a")
      .clearNestedRepeatedField().addAllNestedRepeatedField(jl(10, 20, 30)).build
    val b = RepeatedRecord.newBuilder().setStringField("b")
      .clearNestedRepeatedField().addAllNestedRepeatedField(jl(10, 20, 30)).build
    val c = RepeatedRecord.newBuilder().setStringField("c")
      .clearNestedRepeatedField().addAllNestedRepeatedField(jl(10, 30, 20)).build

    val x = DeeplyRepeatedRecord.newBuilder().clearRepeatedRecord()
      .addAllRepeatedRecord(jl(a, b, c)).build
    val y = DeeplyRepeatedRecord.newBuilder().clearRepeatedRecord()
      .addAllRepeatedRecord(jl(a, c, b)).build
    val z = DeeplyRepeatedRecord.newBuilder().clearRepeatedRecord()
      .addAllRepeatedRecord(jl(a, c)).build

    val du = new ProtoBufDiffy[DeeplyRepeatedRecord](
      unordered = Set("repeated_record", "repeated_record.nested_repeated_field"),
      unorderedFieldKeys = Map("repeated_record" -> "string_field"))

    du(x, y) should equal (Nil)
    du(x, z) should equal (Seq(
      Delta("repeated_record", Option(jl(a, b, c)), Option(jl(a, c)), UnknownDelta)))
  }
}

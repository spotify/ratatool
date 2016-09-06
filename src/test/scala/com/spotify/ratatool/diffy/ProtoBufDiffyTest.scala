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

import com.spotify.ratatool.generators.ProtoBufGenerator
import com.spotify.ratatool.proto.Schemas.{OptionalNestedRecord, RepeatedNestedRecord, TestRecord}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class ProtoBufDiffyTest extends FlatSpec with Matchers {

  def jl[T](x: T*): java.util.List[T] = List(x: _*).asJava

  "ProtoBufDiffy" should "support primitive fields" in {
    val x = OptionalNestedRecord.newBuilder().setInt32Field(10).setInt64Field(20L).build()
    val y = OptionalNestedRecord.newBuilder().setInt32Field(10).setInt64Field(20L).build()
    val z = OptionalNestedRecord.newBuilder().setInt32Field(10).setInt64Field(200L).build()

    ProtoBufDiffy(x, y) should equal (Nil)
    ProtoBufDiffy(x, z) should equal (Seq(Delta("int64_field", 20L, 200L)))
  }

  it should "support nested fields" in {
    val base = ProtoBufGenerator.protoBufOf[TestRecord]

    val onr = ProtoBufGenerator.protoBufOf[OptionalNestedRecord].toBuilder
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
          .setStringField("world")
      ).build()
    val z2 = TestRecord.newBuilder(x).clearOptionalNestedField().build()
    val z3 = TestRecord.newBuilder(x).clearOptionalNestedField().build()

    ProtoBufDiffy(x, y) should equal (Nil)
    ProtoBufDiffy(x, z1) should equal (Seq(
      Delta("optional_nested_field.int64_field", 20L, 200L),
      Delta("optional_nested_field.string_field", "hello", "world")))
    ProtoBufDiffy(x, z2) should equal (Seq(
      Delta("optional_nested_field", onr, null)))
    ProtoBufDiffy(z2, z3) should equal (Nil)
  }

  it should "support repeated fields" in {
    val base = ProtoBufGenerator.protoBufOf[TestRecord]
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
          .addAllInt64Field(jl(20L, 210L))
          .addAllStringField(jl("Hello", "World"))
          .build()
      ).build()

    ProtoBufDiffy(x, y) should equal (Nil)
    ProtoBufDiffy(x, z) should equal (Seq(
      Delta("repeated_fields.int64_field", jl(20L, 21L), jl(20L, 210L)),
      Delta("repeated_fields.string_field", jl("hello", "world"), jl("Hello", "World"))))
  }

}

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

import com.google.api.services.bigquery.model.{TableFieldSchema, TableRow, TableSchema}

import scala.collection.JavaConverters._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TableRowDiffyTest extends AnyFlatSpec with Matchers {

  def jl[T](x: T*): java.util.List[T] = List(x: _*).asJava

  "TableRowDiffy" should "support primitive fields" in {
    val schema = new TableSchema().setFields(jl(
      new TableFieldSchema().setName("field1").setType("INTEGER").setMode("REQUIRED"),
      new TableFieldSchema().setName("field2").setType("INTEGER").setMode("REQUIRED"),
      new TableFieldSchema().setName("field3").setType("INTEGER").setMode("REQUIRED")))
    val x = new TableRow().set("field1", 10).set("field2", 20).set("field3", 30)
    val y = new TableRow().set("field1", 10).set("field2", 20).set("field3", 30)
    val z = new TableRow().set("field1", 10).set("field2", 200).set("field3", 300)

    val d = new TableRowDiffy(schema)
    d(x, y) should equal (Nil)
    d(x, z) should equal (Seq(
      Delta("field2", Option(20), Option(200), NumericDelta(180.0)),
      Delta("field3", Option(30), Option(300), NumericDelta(270.0))))
  }

  it should "support nested fields" in {
    val schema = new TableSchema().setFields(jl(
      new TableFieldSchema().setName("field1").setType("RECORD").setMode("NULLABLE").setFields(jl(
        new TableFieldSchema().setName("field1a").setType("INTEGER").setMode("REQUIRED"),
        new TableFieldSchema().setName("field1b").setType("INTEGER").setMode("REQUIRED"),
        new TableFieldSchema().setName("field1c").setType("STRING").setMode("REQUIRED")))))
    val x = new TableRow()
      .set("field1", new TableRow().set("field1a", 10).set("field1b", 20).set("field1c", "hello"))
    val y = new TableRow()
      .set("field1", new TableRow().set("field1a", 10).set("field1b", 20).set("field1c", "hello"))
    val z1 = new TableRow()
      .set("field1", new TableRow().set("field1a", 10).set("field1b", 200).set("field1c", "Hello"))
    val z2 = new TableRow().set("field1", null)
    val z3 = new TableRow().set("field1", null)

    val d = new TableRowDiffy(schema)
    d(x, y) should equal (Nil)
    d(x, z1) should equal (Seq(
      Delta("field1.field1b", Option(20), Option(200), NumericDelta(180.0)),
      Delta("field1.field1c", Option("hello"), Option("Hello"), StringDelta(1.0))))
    d(x, z2) should equal (Seq(
      Delta("field1", Option(x.get("field1")), None, UnknownDelta)))
    d(z2, z3) should equal (Nil)
  }

  it should "support repeated fields" in {
    val schema = new TableSchema().setFields(jl(
      new TableFieldSchema().setName("field1").setType("INTEGER").setMode("REPEATED"),
      new TableFieldSchema().setName("field2").setType("INTEGER").setMode("REPEATED"),
      new TableFieldSchema().setName("field3").setType("STRING").setMode("REPEATED")))
    val x = new TableRow()
      .set("field1", jl(10, 11)).set("field2", jl(20, 21)).set("field3", jl("hello", "world"))
    val y = new TableRow()
      .set("field1", jl(10, 11)).set("field2", jl(20, 21)).set("field3", jl("hello", "world"))
    val z = new TableRow()
      .set("field1", jl(10, 11)).set("field2", jl(-20, -21)).set("field3", jl("Hello", "World"))

    val d = new TableRowDiffy(schema)
    d(x, y) should equal (Nil)
    d(x, z) should equal (Seq(
      Delta("field2", Option(jl(20, 21)), Option(jl(-20, -21)), VectorDelta(2.0)),
      Delta("field3", Option(jl("hello", "world")), Option(jl("Hello", "World")), UnknownDelta)))
  }

  it should "support ignore" in {
    val schema = new TableSchema().setFields(jl(
      new TableFieldSchema().setName("field1").setType("INTEGER").setMode("REQUIRED"),
      new TableFieldSchema().setName("field2").setType("INTEGER").setMode("REQUIRED"),
      new TableFieldSchema().setName("field3").setType("INTEGER").setMode("REQUIRED")))
    val x = new TableRow().set("field1", 10).set("field2", 20).set("field3", 30)
    val y = new TableRow().set("field1", 10).set("field2", 20).set("field3", 30)
    val z = new TableRow().set("field1", 20).set("field2", 200).set("field3", 300)

    val di = new TableRowDiffy(schema, Set("field1"))
    di(x, y) should equal (Nil)
    di(x, z) should equal (Seq(
      Delta("field2", Option(20), Option(200), NumericDelta(180.0)),
      Delta("field3", Option(30), Option(300), NumericDelta(270.0))))
  }

  it should "support unordered" in {
    val schema = new TableSchema().setFields(jl(
      new TableFieldSchema().setName("field1").setType("RECORD").setMode("REPEATED").setFields(jl(
        new TableFieldSchema().setName("field1a").setType("INTEGER").setMode("REQUIRED"),
        new TableFieldSchema().setName("field1b").setType("INTEGER").setMode("REQUIRED"),
        new TableFieldSchema().setName("field1c").setType("STRING").setMode("REQUIRED")))))

    val a = new TableRow().set("field1a", 10).set("field1b", 100).set("field1c", "hello")
    val b = new TableRow().set("field1a", 20).set("field1b", 200).set("field1c", "world")
    val c = new TableRow().set("field1a", 30).set("field1b", 300).set("field1c", "!")

    val x = new TableRow().set("field1", jl(a, b, c))
    val y = new TableRow().set("field1", jl(a, b, c))
    val z = new TableRow().set("field1", jl(a, c, b))

    val du = new TableRowDiffy(schema, unordered = Set("field1"))
    du(x, y) should equal (Nil)
    du(x, z) should equal (Nil)
    val d = new TableRowDiffy(schema)
    d(x, z) should equal (Seq(
      Delta("field1", Option(jl(a, b, c)), Option(jl(a, c, b)), UnknownDelta)))
  }

  it should "support unordered nested" in {
    val schema = new TableSchema().setFields(jl(
      new TableFieldSchema()
        .setName("repeated_record")
        .setType("RECORD")
        .setMode("REPEATED")
        .setFields(jl(
          new TableFieldSchema()
            .setName("nested_repeated_field")
            .setType("INTEGER")
            .setMode("REPEATED"),
          new TableFieldSchema()
            .setName("string_field")
            .setType("STRING")
            .setMode("REQUIRED")))))

    val a = new TableRow()
      .set("nested_repeated_field", jl(10, 20, 30))
      .set("string_field", "hello")

    val b = new TableRow()
      .set("nested_repeated_field", jl(10, 20, 30))
      .set("string_field", "world")

    val c = new TableRow()
      .set("nested_repeated_field", jl(10, 30, 20))
      .set("string_field", "!")

    val x = new TableRow().set("repeated_record", jl(a, b, c))
    val y = new TableRow().set("repeated_record", jl(a, b, c))
    val z = new TableRow().set("repeated_record", jl(a, c, b))

    val du = new TableRowDiffy(schema,
      unordered = Set("repeated_record", "repeated_record.nested_repeated_field"),
      unorderedFieldKeys = Map("repeated_record" -> "string_field"))
    val d = new TableRowDiffy(schema)


    du(x, y) should equal (Nil)
    du(x, z) should equal (Nil)
    d(x, z) should equal (Seq(
      Delta("repeated_record", Option(jl(a, b, c)), Option(jl(a, c, b)), UnknownDelta)))
  }
}

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
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class TableRowDiffyTest extends FlatSpec with Matchers {

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
      Delta("field2", 20, 200, NumericDelta(180.0)),
      Delta("field3", 30, 300, NumericDelta(270.0))))
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
      Delta("field1.field1b", 20, 200, NumericDelta(180.0)),
      Delta("field1.field1c", "hello", "Hello", StringDelta(1.0))))
    d(x, z2) should equal (Seq(
      Delta("field1", x.get("field1"), null, UnknownDelta)))
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
      Delta("field2", jl(20, 21), jl(-20, -21), VectorDelta(2.0)),
      Delta("field3", jl("hello", "world"), jl("Hello", "World"), UnknownDelta)))
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
      Delta("field2", 20, 200, NumericDelta(180.0)),
      Delta("field3", 30, 300, NumericDelta(270.0))))
  }

}

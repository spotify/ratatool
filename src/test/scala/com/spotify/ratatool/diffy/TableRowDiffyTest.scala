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

    TableRowDiffy(x, y, schema) should equal (Nil)
    TableRowDiffy(x, z, schema) should equal (Seq("field2", "field3"))
  }

  it should "support nested fields" in {
    val schema = new TableSchema().setFields(jl(
      new TableFieldSchema().setName("field1").setType("RECORD").setMode("NULLABLE").setFields(jl(
        new TableFieldSchema().setName("field1a").setType("INTEGER").setMode("REQUIRED"),
        new TableFieldSchema().setName("field1b").setType("INTEGER").setMode("REQUIRED"),
        new TableFieldSchema().setName("field1c").setType("INTEGER").setMode("REQUIRED")))))
    val x = new TableRow()
      .set("field1", new TableRow().set("field1a", 10).set("field1b", 20).set("field1c", 30))
    val y = new TableRow()
      .set("field1", new TableRow().set("field1a", 10).set("field1b", 20).set("field1c", 30))
    val z = new TableRow()
      .set("field1", new TableRow().set("field1a", 10).set("field1b", 200).set("field1c", 300))

    TableRowDiffy(x, y, schema) should equal (Nil)
    TableRowDiffy(x, z, schema) should equal (Seq("field1.field1b", "field1.field1c"))
  }

  it should "support repeated fields" in {
    val schema = new TableSchema().setFields(jl(
      new TableFieldSchema().setName("field1").setType("INTEGER").setMode("REPEATED"),
      new TableFieldSchema().setName("field2").setType("INTEGER").setMode("REPEATED"),
      new TableFieldSchema().setName("field3").setType("INTEGER").setMode("REPEATED")))
    val x = new TableRow()
      .set("field1", jl(10, 11)).set("field2", jl(20, 21)).set("field3", jl(30, 31))
    val y = new TableRow()
      .set("field1", jl(10, 11)).set("field2", jl(20, 21)).set("field3", jl(30, 31))
    val z = new TableRow()
      .set("field1", jl(10, 11)).set("field2", jl(20, 210)).set("field3", jl(30, 310))

    TableRowDiffy(x, y, schema) should equal (Nil)
    TableRowDiffy(x, z, schema) should equal (Seq("field2", "field3"))
  }

}

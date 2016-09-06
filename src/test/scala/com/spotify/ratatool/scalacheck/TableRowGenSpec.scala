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

import com.google.api.services.bigquery.model.TableRow
import com.google.cloud.dataflow.sdk.coders.TableRowJsonCoder
import com.google.cloud.dataflow.sdk.util.CoderUtils
import com.spotify.ratatool.Schemas
import org.scalacheck.Prop.{BooleanOperators, all, forAll}
import org.scalacheck._

import collection.JavaConverters._

object TableRowGenSpec extends Properties("TableRowGen") {

  import TableRowGen._

  val coder = TableRowJsonCoder.of()

  property("round trips") = forAll (TableRowGen.tableRowOf(Schemas.tableSchema)) { m =>
    val base64a = CoderUtils.encodeToBase64(coder, m)
    val base64b = CoderUtils.encodeToBase64(coder, CoderUtils.decodeFromBase64(coder, base64a))
    base64a == base64b
  }

  val nf = "nullable_fields"
  val rf = "repeated_fields"
  val richGen = tableRowOf(Schemas.tableSchema)
    .amend(Gen.choose(10L, 20L))(_.getTableRow(nf).set("int_field"))
    .amend(Gen.choose(10.0, 20.0))(_.getTableRow(nf).set("float_field"))
    .amend(Gen.const(true))(_.getTableRow(nf).set("boolean_field"))
    .amend(Gen.const("hello"))(_.getTableRow(nf).set("string_field"))
    .amend(Gen.listOf(Gen.choose(30L, 40L)))(_.getTableRow(rf).set("int_field"))
    .amend(Gen.listOf(Gen.choose(30.0, 40.0)))(_.getTableRow(rf).set("float_field"))
    .amend(Gen.listOf(Gen.const(false)))(_.getTableRow(rf).set("boolean_field"))
    .amend(Gen.listOf(Gen.const("goodbye")))(_.getTableRow(rf).set("string_field"))

  property("support RichTableRowGen") = forAll (richGen) { r =>
    val nullableFields = r.get(nf).asInstanceOf[TableRow]
    val repeatedFields = r.get(rf).asInstanceOf[TableRow]

    val ni = nullableFields.get("int_field").asInstanceOf[Long]
    val nfl = nullableFields.get("float_field").asInstanceOf[Double]
    val nb = nullableFields.get("boolean_field").asInstanceOf[Boolean]
    val ns = nullableFields.get("string_field").asInstanceOf[String]

    val ri = repeatedFields.get("int_field").asInstanceOf[java.util.List[Long]]
    val rfl = repeatedFields.get("float_field").asInstanceOf[java.util.List[Double]]
    val rb = repeatedFields.get("boolean_field").asInstanceOf[java.util.List[Boolean]]
    val rs = repeatedFields.get("string_field").asInstanceOf[java.util.List[String]]
    
    all(
      "Nullable Int"     |: ni >= 10L && ni <= 20L,
      "Nullable Float"   |: nfl >= 10.0 && nfl <= 20.0,
      "Nullable Boolean" |: nb == true,
      "Nullable String"  |: ns == "hello",
      "Repeated Int"     |: ri.asScala.forall(i => i >= 30L && i <= 40L),
      "Repeated Float"   |: rfl.asScala.forall(i => i >= 30.0 && i <= 40.0),
      "Repeated Boolean" |: rb.asScala.forall(!_),
      "Repeated String"  |: rs.asScala.forall(_ == "goodbye")
    )
  }

}

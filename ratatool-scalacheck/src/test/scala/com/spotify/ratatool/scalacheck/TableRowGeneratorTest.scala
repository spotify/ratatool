/*
 * Copyright 2018 Spotify AB.
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

import com.spotify.ratatool.Schemas
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class TableRowGeneratorTest extends AnyFlatSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  val n = "nullable_fields"
  val r = "required_fields"

  "TableRowGenerator" should "round trip" in {
    forAll(tableRowOf(Schemas.tableSchema)) { m =>
      m.setF(m.getF) shouldBe m
    }
  }

  it should "support RichTableRowGen" in {
    val richGen = tableRowOf(Schemas.tableSchema)
      .amend(Gen.choose(10L, 20L))(_.getRecord(n).set("int_field"))
      .amend(Gen.choose(10.0, 20.0))(_.getRecord(n).set("float_field"))
      .amend(Gen.const(true))(_.getRecord(n).set("boolean_field"))
      .amend(Gen.const("hello"))(
        _.getRecord(n).set("string_field"),
        m => s => m.getRecord(n).set("upper_string_field")(s.asInstanceOf[String].toUpperCase)
      )

    forAll(richGen) { r =>
      val fields = r.get(n).asInstanceOf[java.util.LinkedHashMap[String, Any]]
      fields.get("int_field").asInstanceOf[Long] should (be >= 10L and be <= 20L)
      fields.get("float_field").asInstanceOf[Double] should (be >= 10.0 and be <= 20.0)
      fields.get("boolean_field").asInstanceOf[Boolean] shouldBe true
      fields.get("string_field").asInstanceOf[String] shouldBe "hello"
      fields.get("upper_string_field").asInstanceOf[String] shouldBe "HELLO"
    }
  }

  it should "support RichTableRowTupGen" in {
    val richTupGen = (tableRowOf(Schemas.tableSchema), tableRowOf(Schemas.tableSchema)).tupled
      .amend2(Gen.choose(10L, 20L))(
        _.getRecord(r).set("int_field"),
        a => a.getRecord(r).set("int_field")
      )
      .amend2(Arbitrary.arbString.arbitrary)(
        _.getRecord(r).set("string_field"),
        a => a.getRecord(r).set("string_field")
      )
      .amend2(Arbitrary.arbBool.arbitrary)(
        _.getRecord(r).set("boolean_field"),
        _.getRecord(r).set("boolean_field")
      )

    forAll(richTupGen) { case (a, b) =>
      val ar = a.get(r).asInstanceOf[java.util.LinkedHashMap[String, Any]]
      val br = b.get(r).asInstanceOf[java.util.LinkedHashMap[String, Any]]

      ar.get("int_field") shouldBe br.get("int_field")
      ar.get("string_field") shouldBe br.get("string_field")
      ar.get("boolean_field") shouldBe br.get("boolean_field")
    }
  }

}

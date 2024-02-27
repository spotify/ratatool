/*
 * Copyright 2017 Spotify AB.
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

import com.spotify.ratatool.avro.specific.{RequiredNestedRecord, TestRecord}
import org.scalacheck._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class AvroGeneratorTest extends AnyFlatSpec with Matchers with ScalaCheckPropertyChecks {

  val genTestRecord = specificRecordOf[TestRecord]
  val genRequiredNestedRecord = specificRecordOf[RequiredNestedRecord]

  "AvroGenerator" should "support RichAvroGen" in {
    val genRich = genTestRecord
      .amend(Gen.choose(10, 20))(_.getNullableFields.setIntField)
      .amend(Gen.choose(10L, 20L))(_.getNullableFields.setLongField)
      .amend(Gen.choose(10.0f, 20.0f))(_.getNullableFields.setFloatField)
      .amend(Gen.choose(10.0, 20.0))(_.getNullableFields.setDoubleField)
      .amend(Gen.const(true))(_.getNullableFields.setBooleanField)
      .amend(Gen.const("hello"))(
        _.getNullableFields.setStringField,
        m => s => m.getNullableFields.setUpperStringField(s.toUpperCase)
      )
      .amend(Gen.const(BigDecimal("5.000000001").bigDecimal))(
        _.getRequiredFields.setLogicalDecimalField
      )

    forAll(genRich) { r =>
      r.getNullableFields.getIntField.toInt should (be >= 10 and be <= 20)
      r.getNullableFields.getLongField.toLong should (be >= 10L and be <= 20L)
      r.getNullableFields.getDoubleField.toDouble should (be >= 10.0 and be <= 20.0)
      r.getNullableFields.getBooleanField shouldBe true
      r.getNullableFields.getUpperStringField shouldBe "HELLO"
      r.getRequiredFields.getLogicalDecimalField shouldBe BigDecimal("5.000000001").bigDecimal
    }
  }

  it should "support RichAvroTupGen" in {
    val genRichTup = (genTestRecord, genTestRecord).tupled
      .amend2(genRequiredNestedRecord)(_.setRequiredFields, _.setRequiredFields)

    forAll(genRichTup) { case (a, b) =>
      a.getRequiredFields shouldBe b.getRequiredFields
    }
  }
}

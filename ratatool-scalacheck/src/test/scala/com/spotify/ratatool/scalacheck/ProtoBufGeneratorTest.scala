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

import com.spotify.ratatool.proto.Schemas.{OptionalNestedRecord, RequiredNestedRecord, TestRecord}
import org.scalacheck.Gen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ProtoBufGeneratorTest extends AnyFlatSpec with Matchers with ScalaCheckPropertyChecks {

  "ProtoBufGenerator" should "round trip" in {
    forAll(protoBufOf[TestRecord]) { m =>
      m shouldBe TestRecord.parseFrom(m.toByteArray)
    }
  }

  it should "support RichProtoGen" in {
    val optionalNestedRecordGen: Gen[OptionalNestedRecord] = protoBufOf[OptionalNestedRecord]
      .map(_.toBuilder)
      .amend(Gen.choose(10, 20))(_.setInt32Field)
      .amend(Gen.choose(10L, 20L))(_.setInt64Field)
      .amend(Gen.choose(10.0f, 20.0f))(_.setFloatField)
      .amend(Gen.choose(10.0, 20.0))(_.setDoubleField)
      .amend(Gen.const(true))(_.setBoolField)
      .amend(Gen.const("hello"))(_.setStringField, m => s => m.setUpperStringField(s.toUpperCase))
      .map(_.build())

    val richGen: Gen[TestRecord] = protoBufOf[TestRecord]
      .map(_.toBuilder)
      .amend(optionalNestedRecordGen)(_.setOptionalFields)
      .map(_.build())

    forAll(richGen) { r =>
      r.getOptionalFields.getInt32Field should (be >= 10 and be <= 20)
      r.getOptionalFields.getInt64Field should (be >= 10L and be <= 20L)
      r.getOptionalFields.getFloatField should (be >= 10.0f and be <= 20.0f)
      r.getOptionalFields.getDoubleField should (be >= 10.0 and be <= 20.0)
      r.getOptionalFields.getBoolField shouldBe true
      r.getOptionalFields.getStringField shouldBe "hello"
      r.getOptionalFields.getUpperStringField shouldBe "HELLO"
    }
  }

  it should "support RichProtoTupGen" in {
    val richTupGen =
      (protoBufOf[TestRecord].map(_.toBuilder), protoBufOf[TestRecord].map(_.toBuilder)).tupled
        .amend2(protoBufOf[RequiredNestedRecord])(_.setRequiredFields, _.setRequiredFields)
        .map { case (a, b) => (a.build(), b.build()) }
    forAll(richTupGen) { case (a, b) =>
      a.getRequiredFields shouldBe b.getRequiredFields
    }
  }
}

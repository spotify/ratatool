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
import org.scalacheck.{Gen, Properties}
import org.scalacheck.Prop.{propBoolean, all, forAll}


object ProtoBufGeneratorTest extends Properties("ProtoBufGenerator") {
  property("round trip") = forAll(protoBufOf[TestRecord]) { m =>
    m == TestRecord.parseFrom(m.toByteArray)
  }

  val optionalNestedRecordGen: Gen[OptionalNestedRecord] = protoBufOf[OptionalNestedRecord]
    .map(_.toBuilder)
    .amend(Gen.choose(10, 20))(_.setInt32Field)
    .amend(Gen.choose(10L, 20L))(_.setInt64Field)
    .amend(Gen.choose(10.0f, 20.0f))(_.setFloatField)
    .amend(Gen.choose(10.0, 20.0))(_.setDoubleField)
    .amend(Gen.const(true))(_.setBoolField)
    .amend(Gen.const("hello"))(_.setStringField, m => s => m.setUpperStringField(s.toUpperCase))
    .map(_.build())


  val richGen: Gen[TestRecord] = protoBufOf[TestRecord].map(_.toBuilder)
    .amend(optionalNestedRecordGen)(_.setOptionalFields)
    .map(_.build())

  val richTupGen =
    (protoBufOf[TestRecord].map(_.toBuilder), protoBufOf[TestRecord].map(_.toBuilder)).tupled
    .amend2(protoBufOf[RequiredNestedRecord])(_.setRequiredFields, _.setRequiredFields)
    .map{ case (a, b) => (a.build(), b.build()) }


  property("support RichProtoGen") = forAll (richGen) { r =>
    all(
      "Int" |:
        (r.getOptionalFields.getInt32Field >= 10 && r.getOptionalFields.getInt32Field <= 20),
      "Long" |:
        r.getOptionalFields.getInt64Field >= 10L && r.getOptionalFields.getInt64Field <= 20L,
      "Float" |:
        r.getOptionalFields.getFloatField >= 10.0f && r.getOptionalFields.getFloatField <= 20.0f,
      "Double" |:
        r.getOptionalFields.getDoubleField >= 10.0 && r.getOptionalFields.getDoubleField <= 20.0,
      "Boolean" |: r.getOptionalFields.getBoolField,
      "String" |: r.getOptionalFields.getStringField == "hello",
      "String" |: r.getOptionalFields.getUpperStringField == "HELLO"
    )
  }

  property("support RichProtoTupGen") = forAll (richTupGen) { case (a, b) =>
    (a.getRequiredFields.getBoolField == b.getRequiredFields.getBoolField
      && a.getRequiredFields.getInt32Field == b.getRequiredFields.getInt32Field
      && a.getRequiredFields.getFixed64Field == b.getRequiredFields.getFixed64Field
      && a.getRequiredFields.getStringField == b.getRequiredFields.getStringField
      && a.getRequiredFields.getUint32Field == b.getRequiredFields.getUint32Field)
  }
}

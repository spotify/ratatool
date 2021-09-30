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
import org.apache.beam.sdk.coders.shaded.ScioAvroCoder
import org.apache.beam.sdk.util.CoderUtils
import org.scalacheck._
import org.scalacheck.Prop.{all, forAll, propBoolean, AnyOperators}

object AvroGeneratorTest extends Properties("AvroGenerator") {
  property("round trips") = forAll(specificRecordOf[TestRecord]) { m =>
    val coder = ScioAvroCoder.of(classOf[TestRecord], true)

    val bytes = CoderUtils.encodeToByteArray(coder, m)
    val decoded = CoderUtils.decodeFromByteArray(coder, bytes)
    decoded ?= m
  }

  val richGen = specificRecordOf[TestRecord]
    .amend(Gen.choose(10, 20))(_.getNullableFields.setIntField)
    .amend(Gen.choose(10L, 20L))(_.getNullableFields.setLongField)
    .amend(Gen.choose(10.0f, 20.0f))(_.getNullableFields.setFloatField)
    .amend(Gen.choose(10.0, 20.0))(_.getNullableFields.setDoubleField)
    .amend(Gen.const(true))(_.getNullableFields.setBooleanField)
    .amend(Gen.const("hello"))(
      _.getNullableFields.setStringField,
      m => s => m.getNullableFields.setUpperStringField(s.toUpperCase)
    )

  val richTupGen = (specificRecordOf[TestRecord], specificRecordOf[TestRecord]).tupled
    .amend2(specificRecordOf[RequiredNestedRecord])(_.setRequiredFields, _.setRequiredFields)

  property("support RichAvroGen") = forAll(richGen) { r =>
    all(
      "Int" |:
        r.getNullableFields.getIntField >= 10 && r.getNullableFields.getIntField <= 20,
      "Long" |:
        r.getNullableFields.getLongField >= 10L && r.getNullableFields.getLongField <= 20L,
      "Float" |:
        r.getNullableFields.getFloatField >= 10.0f && r.getNullableFields.getFloatField <= 20.0f,
      "Double" |:
        r.getNullableFields.getDoubleField >= 10.0 && r.getNullableFields.getDoubleField <= 20.0,
      "Boolean" |: r.getNullableFields.getBooleanField == true,
      "String" |: r.getNullableFields.getStringField == "hello",
      "String" |: r.getNullableFields.getUpperStringField == "HELLO"
    )
  }

  property("support RichAvroTupGen") = forAll(richTupGen) { case (a, b) =>
    (a.getRequiredFields.getBooleanField == b.getRequiredFields.getBooleanField
      && a.getRequiredFields.getIntField == b.getRequiredFields.getIntField
      && a.getRequiredFields.getStringField.toString == b.getRequiredFields.getStringField.toString
      && a.getRequiredFields.getLongField == b.getRequiredFields.getLongField)
  }

}

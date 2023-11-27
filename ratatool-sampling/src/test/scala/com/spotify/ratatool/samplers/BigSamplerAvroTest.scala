/*
 * Copyright 2019 Spotify AB.
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

package com.spotify.ratatool.samplers

import com.spotify.ratatool.Schemas
import com.spotify.ratatool.avro.specific.{NullableNestedRecord, TestRecord}
import com.spotify.scio.testing.PipelineSpec
import com.spotify.ratatool.scalacheck._
import org.apache.avro.Conversions
import org.apache.avro.specific.SpecificData
import org.scalacheck.Gen

import scala.util.chaining._

class BigSamplerAvroTest extends PipelineSpec {

  "buildKey" should "not throw NPE if field is null" in {

    val schema = Schemas.avroSchema
    val fields = Seq("nullable_fields.string_field")

    // This is require for avro 1.8 that does not generate valid model data
    val specificData = new SpecificData()
      .tap(_.addLogicalTypeConversion(new Conversions.DecimalConversion()))
    val genTestRecord = specificRecordOf[TestRecord](specificData)
    val genNullableNestedRecord = specificRecordOf[NullableNestedRecord](specificData)

    val record = genTestRecord
      .amend(genNullableNestedRecord.amend(Gen.const(null))(_.setStringField))(_.setNullableFields)
      .sample
      .get

    BigSamplerAvro.buildKey(schema, fields)(record) shouldBe List("null")
  }

}

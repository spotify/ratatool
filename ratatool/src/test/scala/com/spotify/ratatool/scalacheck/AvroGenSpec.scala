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

import com.google.cloud.dataflow.sdk.coders.AvroCoder
import com.google.cloud.dataflow.sdk.util.CoderUtils
import com.spotify.ratatool.avro.specific.TestRecord
import org.scalacheck.Prop.{BooleanOperators, all, forAll}
import org.scalacheck._

object AvroGenSpec extends Properties("AvroGen") {

  import AvroGen._

  val coder = AvroCoder.of(classOf[TestRecord])

  property("round trips") = forAll (avroOf[TestRecord]) { m =>
    val bytes = CoderUtils.encodeToByteArray(coder, m)
    m == CoderUtils.decodeFromByteArray(coder, bytes)
  }

  val richGen = avroOf[TestRecord]
    .amend(Gen.choose(10, 20))(_.getNullableFields.setIntField)
    .amend(Gen.choose(10L, 20L))(_.getNullableFields.setLongField)
    .amend(Gen.choose(10.0f, 20.0f))(_.getNullableFields.setFloatField)
    .amend(Gen.choose(10.0, 20.0))(_.getNullableFields.setDoubleField)
    .amend(Gen.const(true))(_.getNullableFields.setBooleanField)
    .amend(Gen.const("hello"))(_.getNullableFields.setStringField)

  property("support RichAvroGen") = forAll (richGen) { r =>
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
      "String" |: r.getNullableFields.getStringField == "hello"
    )
  }

}

package com.spotify.ratatool.samplers

import com.spotify.ratatool.Schemas
import com.spotify.ratatool.avro.specific.{NullableNestedRecord, TestRecord}
import com.spotify.scio.testing.PipelineSpec
import com.spotify.ratatool.scalacheck._
import org.scalacheck.Gen

class BigSamplerAvroTest extends PipelineSpec {

  "buildKey" should "not throw NPE if field is null" in {

    val schema = Schemas.avroSchema
    val fields = Seq("nullable_fields.string_field")

    val record = specificRecordOf[TestRecord]
      .amend(specificRecordOf[NullableNestedRecord]
        .amend(Gen.const(null))(_.setStringField))(_.setNullableFields)
      .sample.get

    BigSamplerAvro.buildKey(schema, fields)(record) shouldBe Set(null)
  }

}

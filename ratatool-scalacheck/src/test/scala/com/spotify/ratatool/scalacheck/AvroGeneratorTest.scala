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
import com.spotify.ratatool.scalacheck.AvroGeneratorTest.StringableCustom
import org.apache.avro.generic.GenericData
import org.apache.avro.specific.SpecificData
import org.apache.avro.util.Utf8
import org.apache.avro.{Conversions, LogicalTypes, SchemaBuilder}
import org.scalacheck._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

object AvroGeneratorTest {
  final case class StringableCustom(value: String) {
    override def toString: String = value
  }
}

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

  it should "support logical type" in {
    // format: off
    val decimalType = LogicalTypes.decimal(10, 2).addToSchema(SchemaBuilder.builder().bytesType())
    val schema = SchemaBuilder
      .builder()
      .record("TestLogicalType")
      .fields()
      .name("cost").`type`(decimalType).noDefault()
      .endRecord()
    // format: on

    // For generic records, logical type conversion must be explicitly enabled
    GenericData.get().addLogicalTypeConversion(new Conversions.DecimalConversion)

    val gen = avroOf(schema)
    forAll(gen) { r =>
      r.get("cost") shouldBe a[java.math.BigDecimal]
    }
  }

  it should "respect string type" in {
    // format: off
    val schema = SchemaBuilder
      .builder()
      .record("TestStringType")
      .fields()
      .name("defaultStringField").`type`().stringType().noDefault()
      .name("javaStringField").`type`().stringBuilder().prop(GenericData.STRING_PROP, "String").endString().noDefault()
      .name("charSequenceField").`type`().stringBuilder().prop(GenericData.STRING_PROP, "CharSequence").endString().noDefault()
      .name("utf8Field").`type`().stringBuilder().prop(GenericData.STRING_PROP, "CharSequence").endString().noDefault()
      .name("stringableBigDecimalField").`type`().stringBuilder().prop(SpecificData.CLASS_PROP, classOf[java.math.BigDecimal].getName).endString().noDefault()
      .name("stringableBigIntegerField").`type`().stringBuilder().prop(SpecificData.CLASS_PROP, classOf[java.math.BigInteger].getName).endString().noDefault()
      .name("stringableUriField").`type`().stringBuilder().prop(SpecificData.CLASS_PROP, classOf[java.net.URI].getName).endString().noDefault()
      .name("stringableUrlField").`type`().stringBuilder().prop(SpecificData.CLASS_PROP, classOf[java.net.URL].getName).endString().noDefault()
      .name("stringableFileField").`type`().stringBuilder().prop(SpecificData.CLASS_PROP, classOf[java.io.File].getName).endString().noDefault()
      .name("stringableCustomField").`type`().stringBuilder().prop(SpecificData.CLASS_PROP, classOf[StringableCustom].getName).endString().noDefault()
      .name("mapDefaultKeyField").`type`().map().values().longType().noDefault()
      .name("mapJavaStringKeyField").`type`().map().prop(GenericData.STRING_PROP, "String").values().longType().noDefault()
      .name("mapCustomKeyField").`type`().map().prop(SpecificData.KEY_CLASS_PROP, classOf[StringableCustom].getName).values().longType().noDefault()
      .endRecord()
    // format: on

    val gen = avroOf(schema)

    forAll(gen) { r =>
      r.get("defaultStringField") shouldBe an[Utf8]
      r.get("javaStringField") shouldBe a[String]
      r.get("charSequenceField") shouldBe an[Utf8]
      r.get("utf8Field") shouldBe an[Utf8]
      r.get("stringableBigDecimalField") shouldBe a[java.math.BigDecimal]
      r.get("stringableBigIntegerField") shouldBe a[java.math.BigInteger]
      r.get("stringableUriField") shouldBe a[java.net.URI]
      r.get("stringableUrlField") shouldBe a[java.net.URL]
      r.get("stringableFileField") shouldBe a[java.io.File]
      r.get("stringableCustomField") shouldBe a[StringableCustom]

      {
        val m = r.get("mapDefaultKeyField").asInstanceOf[java.util.Map[_, _]]
        if (!m.isEmpty) m.keySet().iterator().next() shouldBe an[Utf8]
      }

      {
        val m = r.get("mapJavaStringKeyField").asInstanceOf[java.util.Map[_, _]]
        if (!m.isEmpty) m.keySet().iterator().next() shouldBe a[String]
      }

      {
        val m = r.get("mapCustomKeyField").asInstanceOf[java.util.Map[_, _]]
        if (!m.isEmpty) m.keySet().iterator().next() shouldBe a[StringableCustom]
      }
    }
  }
}

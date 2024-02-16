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

package com.spotify.ratatool.diffy

import org.apache.avro.util.Utf8
import org.apache.beam.sdk.util.CoderUtils
import org.scalacheck.Gen
import org.scalacheck.rng.Seed
import com.spotify.ratatool.avro.specific.{RequiredNestedRecord, TestRecord}
import com.spotify.ratatool.scalacheck._
import com.spotify.scio.avro._
import com.spotify.scio.coders.kryo._
import com.spotify.scio.testing.PipelineSpec
import com.google.api.services.bigquery.model.{TableFieldSchema, TableRow, TableSchema}
import com.spotify.ratatool.diffy.BigDiffy.{avroKeyFn, mergeTableSchema, stripQuoteWrap}
import com.spotify.ratatool.io.{ParquetIO, ParquetTestData}
import com.spotify.scio.ScioContext
import com.spotify.scio.coders.{Coder, CoderMaterializer}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder

import scala.jdk.CollectionConverters._
import scala.language.higherKinds

class BigDiffyTest extends PipelineSpec {

  val keys = (1 to 1000).map(k => MultiKey("key" + k))
  val coder = CoderMaterializer.beamWithDefault(Coder[TestRecord])

  /** Fixed to a small range so that Std. Dev. & Variance calculations are easier to predict */
  val rnr = specificRecordOf[RequiredNestedRecord]
    .amend(Gen.choose[Double](0.0, 1.0))(_.setDoubleField)
  val specificGen = specificRecordOf[TestRecord]
    .amend(rnr)(_.setRequiredFields)
  val lhs = Gen
    .listOfN(1000, specificGen)
    .pureApply(Gen.Parameters.default.withSize(10), Seed.random())
    .zip(1 to 1000)
    .map { case (r, i) =>
      r.getRequiredFields.setIntField(i)
      r.getRequiredFields.setStringField(new Utf8("key" + i))
      r
    }
  val field = "required_fields.double_field"

  "BigDiffy" should "work with identical inputs" in {
    runWithContext { sc =>
      val rhs = lhs.map(CoderUtils.clone(coder, _))
      val result = BigDiffy.diff[TestRecord](
        sc.parallelize(lhs),
        sc.parallelize(rhs),
        new AvroDiffy[TestRecord](),
        x => MultiKey(x.getRequiredFields.getStringField.toString)
      )
      result.globalStats should containSingleValue(GlobalStats(1000L, 1000L, 0L, 0L, 0L))
      result.deltas should beEmpty
      result.keyStats should containInAnyOrder(keys.map(KeyStats(_, DiffType.SAME, None)))
      result.fieldStats should beEmpty
    }
  }

  it should "work with deltas" in {
    runWithContext { sc =>
      val keyedDoubles = lhs.map { i =>
        (MultiKey(i.getRequiredFields.getStringField.toString), i.getRequiredFields.getDoubleField)
      }
      val rhs = lhs.map(CoderUtils.clone(coder, _)).map { r =>
        r.getRequiredFields.setDoubleField(r.getRequiredFields.getDoubleField + 10.0)
        r
      }
      val result = BigDiffy.diff[TestRecord](
        sc.parallelize(lhs),
        sc.parallelize(rhs),
        new AvroDiffy[TestRecord](),
        x => MultiKey(x.getRequiredFields.getStringField.toString)
      )
      result.globalStats should containSingleValue(GlobalStats(1000L, 0L, 1000L, 0L, 0L))
      result.deltas.map(d => (d._1, d._2)) should containInAnyOrder(keys.map((_, field)))
      result.keyStats should containInAnyOrder(keyedDoubles.map { case (k, d) =>
        KeyStats(
          k,
          DiffType.DIFFERENT,
          Option(
            Delta(
              "required_fields.double_field",
              Option(d),
              Option(d + 10.0),
              TypedDelta(DeltaType.NUMERIC, 10.0)
            )
          )
        )
      })
      result.fieldStats.map(f => (f.field, f.count, f.fraction)) should containSingleValue(
        (field, 1000L, 1.0)
      )
      // Double.NaN comparison is always false
      val deltaStats = result.fieldStats
        .flatMap(_.deltaStats)
        .map(d => (d.deltaType, d.min, d.max, d.count, d.mean, d.variance, d.stddev))
      deltaStats should containSingleValue((DeltaType.NUMERIC, 10.0, 10.0, 1000L, 10.0, 0.0, 0.0))
    }
  }

  it should "work with missing LHS" in {
    runWithContext { sc =>
      val lhs2 = lhs.filter(_.getRequiredFields.getIntField <= 500)
      val rhs = lhs.map(CoderUtils.clone(coder, _))
      val result = BigDiffy.diff[TestRecord](
        sc.parallelize(lhs2),
        sc.parallelize(rhs),
        new AvroDiffy[TestRecord](),
        x => MultiKey(x.getRequiredFields.getStringField.toString)
      )
      result.globalStats should containSingleValue(GlobalStats(1000L, 500L, 0L, 500L, 0L))
      result.deltas should beEmpty
      result.keyStats should containInAnyOrder(
        (1 to 500).map(i => KeyStats(MultiKey("key" + i), DiffType.SAME, None)) ++
          (501 to 1000).map(i => KeyStats(MultiKey("key" + i), DiffType.MISSING_LHS, None))
      )
      result.fieldStats should beEmpty
    }
  }

  it should "work with missing RHS" in {
    runWithContext { sc =>
      val rhs = lhs.filter(_.getRequiredFields.getIntField <= 500).map(CoderUtils.clone(coder, _))
      val result = BigDiffy.diff[TestRecord](
        sc.parallelize(lhs),
        sc.parallelize(rhs),
        new AvroDiffy[TestRecord](),
        x => MultiKey(x.getRequiredFields.getStringField.toString)
      )
      result.globalStats should containSingleValue(GlobalStats(1000L, 500L, 0L, 0L, 500L))
      result.deltas should beEmpty
      result.keyStats should containInAnyOrder(
        (1 to 500).map(i => KeyStats(MultiKey("key" + i), DiffType.SAME, None)) ++
          (501 to 1000).map(i => KeyStats(MultiKey("key" + i), DiffType.MISSING_RHS, None))
      )
      result.fieldStats should beEmpty
    }
  }

  it should "ignore NaNs for field stats when ignoreNan flag is on" in {
    runWithContext { sc =>
      val rhs = lhs
        .map(CoderUtils.clone(coder, _))
        .map { r =>
          val intField = r.getRequiredFields.getIntField
          if (intField > 900 && intField <= 950) {
            r.getRequiredFields.setDoubleField(r.getRequiredFields.getDoubleField + 10.0)
          } else if (intField > 950) {
            r.getRequiredFields.setDoubleField(Double.NaN)
          }
          r
        }
      val ignoreNan = true
      val result = BigDiffy.diff[TestRecord](
        sc.parallelize(lhs),
        sc.parallelize(rhs),
        new AvroDiffy[TestRecord](),
        x => MultiKey(x.getRequiredFields.getStringField.toString),
        ignoreNan
      )
      result.globalStats should containSingleValue(GlobalStats(1000L, 900L, 100L, 0L, 0L))
      result.deltas.map(d => (d._1, d._2)) should containInAnyOrder(
        (901 to 1000).map(k => MultiKey("key" + k)).map((_, field))
      )
      val deltaStats = result.fieldStats
        .flatMap(_.deltaStats)
        .map(d => (d.deltaType, d.min, d.max, d.count, d.mean, d.variance, d.stddev))
      deltaStats should containSingleValue((DeltaType.NUMERIC, 10.0, 10.0, 50L, 10.0, 0.0, 0.0))
    }
  }

  it should "return NaN for field stats when ignoreNan flag is off" in {
    runWithContext { sc =>
      val rhs = lhs
        .map(CoderUtils.clone(coder, _))
        .map { r =>
          val intField = r.getRequiredFields.getIntField
          if (intField > 900 && intField <= 950) {
            r.getRequiredFields.setDoubleField(r.getRequiredFields.getDoubleField + 10.0)
          } else if (intField > 950) {
            r.getRequiredFields.setDoubleField(Double.NaN)
          }
          r
        }
      val result = BigDiffy.diff[TestRecord](
        sc.parallelize(lhs),
        sc.parallelize(rhs),
        new AvroDiffy[TestRecord](),
        x => MultiKey(x.getRequiredFields.getStringField.toString)
      )
      result.globalStats should containSingleValue(GlobalStats(1000L, 900L, 100L, 0L, 0L))
      result.deltas.map(d => (d._1, d._2)) should containInAnyOrder(
        (901 to 1000).map(k => MultiKey("key" + k)).map((_, field))
      )
      val deltaStats = result.fieldStats
        .flatMap(_.deltaStats)
        // checking d.min.isNaN and d.max.isNaN will result in flaky tests since
        // Min(2.0) + Min(1.0) + Min(Double.NaN) = Min(1.0) while
        // Min(Double.NaN) + Min(2.0) + Min(1.0) = Min(NaN) in algebird.
        .map(d => (d.deltaType, d.count, d.mean.isNaN, d.variance.isNaN, d.stddev.isNaN))
      deltaStats should containSingleValue((DeltaType.NUMERIC, 100L, true, true, true))
    }
  }

  a[RuntimeException] shouldBe thrownBy {
    runWithContext { sc =>
      val lhsDuplicate = Gen
        .listOfN(2, specificGen)
        .sample
        .get
        .map { r =>
          r.getRequiredFields.setIntField(10)
          r.getRequiredFields.setStringField("key")
          r
        }
      val rhs = Gen
        .listOfN(1, specificGen)
        .sample
        .get
        .map { r =>
          r.getRequiredFields.setIntField(10)
          r.getRequiredFields.setStringField("key")
          r
        }
      val result = BigDiffy.diff[TestRecord](
        sc.parallelize(lhsDuplicate),
        sc.parallelize(rhs),
        new AvroDiffy[TestRecord](),
        x => MultiKey(x.getRequiredFields.getStringField.toString)
      )
      val res = result.deltas.map(_._1)
    }
  }

  "BigDiffy avroKeyFn" should "work with nullable key" in {
    val record = specificRecordOf[TestRecord].sample.get
    record.getNullableFields.setIntField(null)
    val keyValue = BigDiffy.avroKeyFn(Seq("nullable_fields.int_field"))(record)

    keyValue.toString shouldBe "null"
  }

  "BigDiffy avroKeyFn" should "work with single key" in {
    val record = specificRecordOf[TestRecord].sample.get
    val keyValue = BigDiffy.avroKeyFn(Seq("required_fields.int_field"))(record)

    keyValue.toString shouldBe record.getRequiredFields.getIntField.toString
  }

  "BigDiffy avroKeyFn" should "work with multiple key" in {
    val record = specificRecordOf[TestRecord].sample.get
    val keys = Seq("required_fields.int_field", "required_fields.double_field")
    val keyValues = BigDiffy.avroKeyFn(keys)(record)
    val expectedKey =
      s"${record.getRequiredFields.getIntField}_${record.getRequiredFields.getDoubleField}"

    keyValues.toString shouldBe expectedKey
  }

  "stripQuoteWrap" should "strip matching start and end quotes" in {
    stripQuoteWrap("\"abc\"") shouldEqual "abc"
    stripQuoteWrap("`abc`") shouldEqual "abc"
    stripQuoteWrap("'abc'") shouldEqual "abc"
  }

  "stripQuoteWrap" should "leave anything else" in {
    stripQuoteWrap("date=\"2021-12-01\"") shouldEqual "date=\"2021-12-01\""
    stripQuoteWrap("date='2021-12-01'") shouldEqual "date='2021-12-01'"
    stripQuoteWrap("abc") shouldEqual "abc"
  }

  "BigDiffy tableRowKeyFn" should "work with single key" in {
    val record = new TableRow()
    record.set("key", "foo")
    val keyValue = BigDiffy.tableRowKeyFn(Seq("key"))(record)

    keyValue.toString shouldBe "foo"
  }

  "BigDiffy tableRowKeyFn" should "work with multiple key" in {
    val subRecord = new TableRow()
    subRecord.set("key", "foo")
    subRecord.set("other_key", "bar")
    val record = new TableRow()
    record.set("record", subRecord)

    val keys = Seq("record.key", "record.other_key")
    val keyValues = BigDiffy.tableRowKeyFn(keys)(record.asInstanceOf[TableRow])

    keyValues.toString shouldBe "foo_bar"
  }

  "BigDiffy unorderedKeysMap" should "work with multiple unordered keys" in {
    val keyMappings = List("record.nested_record:key", "record.other_nested_record:other_key")
    val unorderedKeys = BigDiffy.unorderedKeysMap(keyMappings)

    unorderedKeys.isSuccess shouldBe true
    unorderedKeys.get shouldBe Map(
      "record.nested_record" -> "key",
      "record.other_nested_record" -> "other_key"
    )
  }

  it should "encode and decode a TableRow when there is a NaN value" in {
    val testFieldName = "test_field"
    val numFieldName = "num"
    val tblRow = new TableRow()
      .set(testFieldName, "a")
      .set(numFieldName, Double.NaN)

    val coder = TableRowJsonCoder.of()
    val encoded = CoderUtils.encodeToByteArray(coder, tblRow)
    new String(encoded) shouldBe "{\"test_field\":\"a\",\"num\":\"NaN\"}"

    val decoded = CoderUtils.decodeFromByteArray(coder, encoded)
    decoded.get(testFieldName) shouldBe "a"
    decoded.get(numFieldName).toString shouldBe "NaN"
  }

  it should "throw an exception when in GCS output mode and output is not gs://" in {
    val exc = the[Exception] thrownBy {
      val args = Array(
        "--runner=DataflowRunner",
        "--project=fake",
        "--tempLocation=gs://tmp/tmp", // dataflow args
        "--input-mode=avro",
        "--key=tmp",
        "--lhs=gs://tmp/lhs",
        "--rhs=gs://tmp/rhs",
        "--output=abc" // no gs:// prefix
        // if no output-mode. defaults to GCS
      )
      BigDiffy.run(args)
    }

    exc.getMessage shouldBe "Output mode is GCS, but output abc is not a valid GCS location"
  }

  it should "throw an exception when rowRestriction is specified for an avro input" in {
    val exc = the[IllegalArgumentException] thrownBy {
      val args = Array(
        "--runner=DataflowRunner",
        "--project=fake",
        "--tempLocation=gs://tmp/tmp", // dataflow args
        "--input-mode=avro",
        "--key=tmp",
        "--lhs=gs://tmp/lhs",
        "--rhs=gs://tmp/rhs",
        "--rowRestriction=true",
        "--output=gs://abc"
      )
      BigDiffy.run(args)
    }

    exc.getMessage shouldBe "rowRestriction cannot be passed for avro inputs"
  }

  it should "throw an exception when rowRestriction is specified for a parquet input" in {
    val exc = the[IllegalArgumentException] thrownBy {
      val args = Array(
        "--runner=DataflowRunner",
        "--project=fake",
        "--tempLocation=gs://tmp/tmp", // dataflow args
        "--input-mode=parquet",
        "--key=tmp",
        "--lhs=gs://tmp/lhs",
        "--rhs=gs://tmp/rhs",
        "--rowRestriction=true",
        "--output=gs://abc"
      )
      BigDiffy.run(args)
    }

    exc.getMessage shouldBe "rowRestriction cannot be passed for Parquet inputs"
  }

  it should "support Parquet schema evolution" in {
    val schema1 = new Schema.Parser().parse(
      """|{"type":"record",
       |"name":"ParquetRecord",
       |"namespace":"com.spotify.ratatool.diffy",
       |"fields":[{"name":"id","type":"int"}]}
       """.stripMargin
    )
    // Schema 2 has added field with default value
    val schema2 = new Schema.Parser().parse(
      """|{"type":"record",
       |"name":"ParquetRecord",
       |"namespace":"com.spotify.ratatool.diffy",
       |"fields":[
       |{"name":"id","type":"int"},
       |{"name":"s","type":["null","string"],"default":null}]}
       """.stripMargin
    )

    def toGenericRecord(schema: Schema, fields: Map[String, _]): GenericRecord = {
      val gr = new GenericData.Record(schema)
      fields.foreach { case (k, v) => gr.put(k, v) }
      gr
    }
    val (lhs, rhs) = (
      ParquetTestData.createTempDir("lhs") + "/out.parquet",
      ParquetTestData.createTempDir("rhs") + "/out.parquet"
    )

    ParquetIO.writeToFile(
      (1 to 10).map(i => toGenericRecord(schema1, Map("id" -> i))),
      schema1,
      rhs
    )

    ParquetIO.writeToFile(
      (1 to 9).map(i => toGenericRecord(schema2, Map("id" -> i, "s" -> i.toString))),
      schema2,
      lhs
    )

    val sc = ScioContext()

    implicit val coder = avroGenericRecordCoder(schema2)

    val bigDiffy =
      BigDiffy.diffParquet(sc, lhs, rhs, avroKeyFn(Seq("id")), new AvroDiffy[GenericRecord]())

    bigDiffy.keyStats.filter(_.diffType == DiffType.MISSING_LHS) should haveSize(1)
    sc.run()
  }

  "mergeTableSchema" should "merge two schemas" in {

    def jl[T](x: T*): java.util.List[T] = List(x: _*).asJava

    val schema1 = new TableSchema().setFields(
      jl(
        new TableFieldSchema().setName("field1").setType("INTEGER").setMode("REQUIRED"),
        new TableFieldSchema()
          .setName("field2")
          .setType("RECORD")
          .setMode("NULLABLE")
          .setFields(
            jl(
              new TableFieldSchema().setName("field2a").setType("INTEGER").setMode("REQUIRED"),
              new TableFieldSchema().setName("field2b").setType("INTEGER").setMode("REQUIRED"),
              new TableFieldSchema().setName("field2c").setType("STRING").setMode("REQUIRED")
            )
          )
      )
    )
    val schema2 = new TableSchema().setFields(
      jl(
        new TableFieldSchema().setName("field1").setType("INTEGER").setMode("REQUIRED"),
        new TableFieldSchema().setName("field3").setType("STRING").setMode("REQUIRED")
      )
    )

    val expected = new TableSchema().setFields(
      jl(
        new TableFieldSchema().setName("field1").setType("INTEGER").setMode("REQUIRED"),
        new TableFieldSchema()
          .setName("field2")
          .setType("RECORD")
          .setMode("NULLABLE")
          .setFields(
            jl(
              new TableFieldSchema().setName("field2a").setType("INTEGER").setMode("REQUIRED"),
              new TableFieldSchema().setName("field2b").setType("INTEGER").setMode("REQUIRED"),
              new TableFieldSchema().setName("field2c").setType("STRING").setMode("REQUIRED")
            )
          ),
        new TableFieldSchema().setName("field3").setType("STRING").setMode("REQUIRED")
      )
    )

    val actual = mergeTableSchema(schema2, schema1)

    assert(actual.getFields.containsAll(expected.getFields))
  }
}

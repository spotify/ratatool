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

package com.spotify.ratatool.examples.scalacheck

import java.util

import com.google.api.client.json.JsonObjectParser
import com.google.api.client.json.gson.GsonFactory
import com.google.api.services.bigquery.model.{TableRow, TableSchema}
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Charsets
import com.spotify.ratatool.avro.specific.{EnumField, ExampleRecord, NestedExampleRecord}
import com.spotify.ratatool.scalacheck._
import org.apache.avro.util.Utf8
import org.scalacheck.{Arbitrary, Gen}
import scala.jdk.CollectionConverters._

object ExampleAvroGen {
  private val utfGen: Gen[Utf8] = Arbitrary.arbString.arbitrary.map(new Utf8(_))

  private val kvGen: Gen[(Utf8, Int)] = for {
    k <- utfGen
    v <- Arbitrary.arbInt.arbitrary
  } yield (k, v)

  /** Generates a map of size 1-5 */
  private val sizedMapGen: Gen[util.Map[CharSequence, java.lang.Integer]] =
    Gen.mapOfN(5, kvGen).map { m =>
      val map = new util.HashMap[Utf8, java.lang.Integer]()
      m.foreach { case (k, v) => map.put(k, v) }
      map.asInstanceOf[util.Map[CharSequence, java.lang.Integer]]
    }

  private val nestedRecordGen: Gen[NestedExampleRecord] = specificRecordOf[NestedExampleRecord]
    .amend(sizedMapGen)(_.setMapField)

  private val boundedDoubleGen: Gen[Double] = Gen.chooseNum(-1.0, 1.0)

  private val intGen: Gen[Int] = Arbitrary.arbInt.arbitrary

  private val errorGen: Gen[String] = for {
    e <- Gen.const("Exception: Ratatool Exception. ")
    m <- Gen.alphaNumStr
  } yield e + m

  private val stringGen: Gen[String] = Gen.oneOf(Gen.alphaNumStr, errorGen)

  /** This and dependentEnumFunc are used to produce fields based on critera */
  private def dependentIntFunc(i: Int): Int = {
    if (i == 0) {
      Int.MaxValue
    } else {
      i / 2
    }
  }

  private def dependentEnumFunc(s: String): EnumField = {
    if (s.length > 0 && s.startsWith("Exception")) {
      EnumField.Failure
    } else {
      EnumField.Success
    }
  }

  /**
   * An example of generating avro data with specific requirements.
   *
   * See [[com.spotify.ratatool.avro.specific.ExampleRecord]] for documentation on field
   * requirements, dependencies, and bounds.
   */
  val exampleRecordGen: Gen[ExampleRecord] =
    specificRecordOf[ExampleRecord]
      .amend(nestedRecordGen)(_.setNestedRecordField)
      .amend(boundedDoubleGen)(_.setBoundedDoubleField)
      .amend(Gen.uuid.map(_.toString))(_.setRecordId)

      /**
       * Set dependent fields based on Schema criteria. This is done in a single amend with a single
       * gen to ensure values are consistent per record
       */
      .amend(intGen)(
        _.setIndependentIntField,
        m => i => m.setDependentIntField(dependentIntFunc(i))
      )
      .amend(stringGen)(
        _.setIndependentStringField,
        m => s => m.setDependentEnumField(dependentEnumFunc(s))
      )

  private val recordIdGen: Gen[String] = Gen.alphaUpperStr

  private val exampleRecordGenDup: Gen[ExampleRecord] =
    specificRecordOf[ExampleRecord]

  val exampleRecordAmend2Gen: Gen[(ExampleRecord, ExampleRecord)] =
    (exampleRecordGen, exampleRecordGenDup).tupled
      .amend2(recordIdGen)(_.setRecordId, _.setRecordId)

  val correlatedRecordGen: Gen[ExampleRecord] =
    (exampleRecordGen, specificRecordOf[NestedExampleRecord]).tupled
      .amend2(recordIdGen)(_.setRecordId, _.setParentRecordId)
      .map { case (exampleRecord, nestedExampleRecord) =>
        exampleRecord.setNestedRecordField(nestedExampleRecord)
        exampleRecord
      }

}

object ExampleTableRowGen {
  private val tableSchema = new JsonObjectParser(new GsonFactory)
    .parseAndClose(
      this.getClass.getResourceAsStream("/schema.json"),
      Charsets.UTF_8,
      classOf[TableSchema]
    )

  private val childSchema = new JsonObjectParser(new GsonFactory)
    .parseAndClose(
      this.getClass.getResourceAsStream("/child.json"),
      Charsets.UTF_8,
      classOf[TableSchema]
    )

  private val freqGen: Gen[String] = Gen.frequency(
    (2, Gen.oneOf("Foo", "Bar")),
    (1, Gen.oneOf("Fizz", "Buzz"))
  )

  private val intListGen: Gen[java.util.List[Int]] = Gen
    .listOfN(
      3,
      Arbitrary.arbInt.arbitrary
    )
    .map(_.asJava)

  /**
   * Nested record Generator where one field depends on another (therefore have to have the same gen
   * and be set in the same fn)
   */
  private val rrGen: Gen[TableRow] = Arbitrary.arbString.arbitrary.map { s =>
    val bytes = s.getBytes(Charsets.UTF_8)
    val t = new TableRow()
    t.set("independent_string_field", s)
    t.set("dependent_bytes_field", bytes)
    t
  }

  /**
   * An example of generating BigQuery table row with specific requirements.
   *
   * See `resources/schema.json` for schema and documentation on requirements.
   */
  val tableRowGen: Gen[TableRow] =
    tableRowOf(tableSchema)
      .amend(rrGen)(_.set("required_record"))

      /**
       * Since nullable_record may not exist, we use tryAmend so that it fails silently if it does
       * not exist
       */
      .tryAmend(intListGen)(_.getRecord("nullable_record").set("repeated_int_field"))
      .tryAmend(freqGen)(_.getRecord("nullable_record").set("frequency_string_field"))

  private val recordIdGen: Gen[String] = Gen.alphaUpperStr

  private val tableRowGenDup: Gen[TableRow] =
    tableRowOf(tableSchema)

  val exampleRecordAmend2Gen: Gen[(TableRow, TableRow)] =
    (tableRowGen, tableRowGenDup).tupled
      .amend2(recordIdGen)(_.set("record_id"), _.set("record_id"))

  val correlatedRecordGen: Gen[TableRow] =
    (tableRowGen, tableRowOf(childSchema)).tupled
      .amend2(recordIdGen)(_.set("record_id"), _.set("parent_record_id"))
      .map { case (record, child) => record.set("parent_record_id", child.get("parent_record_id")) }

}

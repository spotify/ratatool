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

package com.spotify.ratatool.io

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File}
import com.spotify.ratatool.Schemas
import com.spotify.ratatool.avro.specific.TestRecord
import org.apache.avro.generic.GenericRecord
import com.spotify.ratatool.scalacheck._
import org.apache.avro.Conversions
import org.apache.avro.specific.SpecificData
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.chaining._

class AvroIOTest extends AnyFlatSpec with Matchers {

  private val genericSchema = Schemas.avroSchema
  private val genericGen = genericRecordOf(genericSchema)
  private val genericRecords = (1 to 100).flatMap(_ => genericGen.sample)

  // This is require for avro 1.8 that does not generate valid model data
  val specificData = new SpecificData()
    .tap(_.addLogicalTypeConversion(new Conversions.DecimalConversion()))
  private val specificSchema = TestRecord.getClassSchema
  private val specificGen = specificRecordOf[TestRecord](specificData)
  private val specificRecords = (1 to 100).flatMap(_ => specificGen.sample)

  "AvroIO" should "work with generic record and stream" in {
    val out = new ByteArrayOutputStream()
    AvroIO.writeToOutputStream(genericRecords, genericSchema, out)
    val in = new ByteArrayInputStream(out.toByteArray)
    val result = AvroIO.readFromInputStream[GenericRecord](in).toList
    result should equal(genericRecords)
  }

  it should "work with generic record and file" in {
    val file = File.createTempFile("ratatool-", ".avro")
    file.deleteOnExit()
    AvroIO.writeToFile(genericRecords, genericSchema, file)
    val result = AvroIO.readFromFile[GenericRecord](file).toList
    result should equal(genericRecords)
  }

  it should "work with specific record and stream" in {
    val out = new ByteArrayOutputStream()
    AvroIO.writeToOutputStream(specificRecords, specificSchema, out)
    val in = new ByteArrayInputStream(out.toByteArray)
    val result = AvroIO.readFromInputStream[TestRecord](in).toList
    result.map(FixRandomData(_)) should equal(specificRecords.map(FixRandomData(_)))
  }

  it should "work with specific record and file" in {
    val file = File.createTempFile("ratatool-", ".avro")
    file.deleteOnExit()
    AvroIO.writeToFile(specificRecords, specificSchema, file)
    val result = AvroIO.readFromFile[TestRecord](file).toList
    result.map(FixRandomData(_)) should equal(specificRecords.map(FixRandomData(_)))
  }
}

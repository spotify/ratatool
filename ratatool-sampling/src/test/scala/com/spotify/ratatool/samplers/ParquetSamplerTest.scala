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

package com.spotify.ratatool.samplers

import com.spotify.scio.parquet.avro._
import com.spotify.scio.parquet.types._
import com.spotify.scio.ScioContext
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files
import scala.jdk.CollectionConverters._

class ParquetSamplerTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  import TestData._

  override protected def beforeAll(): Unit = {
    TestData.writeTestData()
  }

  "ParquetSampler" should "sample typed parquet records from wildcard pattern" in {
    val sampledHead = new ParquetSampler(s"$typedOut/*.parquet").sample(10, head = true)
    sampledHead should have size 10
    all(sampledHead.map(_.get("id").toString.toInt)) should (be < 100 and be >= 0)

    val sampledNotHead = new ParquetSampler(s"$typedOut/*.parquet").sample(10, head = false)
    sampledNotHead should have size 10
    all(sampledHead.map(_.get("id").toString.toInt)) should (be < 100 and be >= 0)
  }

  it should "sample typed parquet records from non-wildcard pattern" in {
    val sampled =
      new ParquetSampler(s"$typedOut/part-00000-of-00002.parquet").sample(10, head = true)
    sampled should have size 10
    all(sampled.map(_.get("id").toString.toInt)) should (be < 100 and be >= 0)
  }

  it should "sample parquet-avro records as GenericRecords from wildcard pattern" in {
    val sampledHead = new ParquetSampler(s"$avroOut/*.parquet").sample(10, head = true)
    sampledHead should have size 10
    all(sampledHead.map(_.get("id").toString.toInt)) should (be < 100 and be >= 0)

    val sampledNotHead = new ParquetSampler(s"$avroOut/*.parquet").sample(10, head = false)
    sampledNotHead should have size 10
    all(sampledNotHead.map(_.get("id").toString.toInt)) should (be < 100 and be >= 0)
  }

  it should "sample parquet-avro records from non-wildcard pattern" in {
    val sampled =
      new ParquetSampler(s"$avroOut/part-00000-of-00002.parquet").sample(10, head = true)
    sampled should have size 10
    all(sampled.map(_.get("id").toString.toInt)) should (be < 100 and be >= 0)
  }
}

object TestData extends Serializable {
  case class ParquetTyped(id: Int)

  lazy val (typedOut, avroOut) = {
    val typed = Files.createTempDirectory("typedOut")
    val avro = Files.createTempDirectory("avroOut")

    List(typed, avro).foreach { dir =>
      dir.resolve("part-00000-of-00002.parquet").toFile.deleteOnExit()
      dir.resolve("part-00001-of-00002.parquet").toFile.deleteOnExit()
      dir.toFile.deleteOnExit()
    }

    (typed.toString, avro.toString)
  }


  def avroSchema: Schema = Schema.createRecord("ParquetAvro", "com.spotify", "", false,
    List(new Field("id", Schema.create(Schema.Type.INT), "", -1)).asJava)

  def writeTestData(): Unit = {
    def createGr(id: Int): GenericRecord = {
      val gr = new GenericData.Record(avroSchema)
      gr.put("id", id)
      gr.asInstanceOf[GenericRecord]
    }

    val sc = ScioContext()

    // Write typed Parquet records
    sc.parallelize(0 until 100).map(ParquetTyped)
      .saveAsTypedParquetFile(typedOut, numShards = 2)

    // Write avro Parquet records
    sc.parallelize(0 until 100).map(createGr)
      .saveAsParquetAvroFile(avroOut, avroSchema, numShards = 2)

    sc.run()
  }
}
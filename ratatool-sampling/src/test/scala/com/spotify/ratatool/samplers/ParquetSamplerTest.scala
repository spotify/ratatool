/*
 * Copyright 2022 Spotify AB.
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
import com.spotify.scio.coders.Coder
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.Files

class ParquetSamplerTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private lazy val (typedOut, avroOut) = (
    ParquetTestData.createTempDir("typed"),
    ParquetTestData.createTempDir("avro"))

  override protected def beforeAll(): Unit = {
    ParquetTestData.writeTestData(avroPath = avroOut, typedPath = typedOut, numShards = 2)
  }

  private val BeBetween0And100 = (be < 100 and be >= 0)
  private val GetId: GenericRecord => Int = _.get("id").asInstanceOf[Int]

  "ParquetSampler" should "sample typed parquet records" in {
    // Wildcard pattern
    testSampler(new ParquetSampler(s"$typedOut/*.parquet"), head = false)

    // Non-wildcard pattern
    testSampler(new ParquetSampler(s"$typedOut/part-00000-of-00002.parquet"), head = false)

    // Head=true
    testSampler(new ParquetSampler(s"$typedOut/part-00000-of-00002.parquet"), head = true)
  }

  it should "sample parquet-avro records" in {
    // Wildcard pattern
    testSampler(new ParquetSampler(s"$avroOut/*.parquet"), head = false)

    // Non-wildcard pattern
    testSampler(new ParquetSampler(s"$avroOut/part-00000-of-00002.parquet"), head = false)

    // Head=true
    testSampler(new ParquetSampler(s"$avroOut/part-00000-of-00002.parquet"), head = true)
  }

  private def testSampler(parquetSampler: ParquetSampler, head: Boolean): Unit = {
    val sampled = parquetSampler.sample(10, head = head)
    sampled should have size 10
    all(sampled.map(GetId)) should BeBetween0And100
  }
}

object ParquetTestData extends Serializable {
  case class ParquetClass(id: Int)

  def createTempDir(prefix: String): String = {
    val dir = Files.createTempDirectory(prefix)
    dir.toFile.deleteOnExit()
    dir.toString
  }

  lazy val ParquetAvroData: Seq[GenericRecord] = (0 until 100).map { id =>
    val gr = new GenericData.Record(avroSchema)
    gr.put("id", id)
    gr
  }

  lazy val ParquetTypedData: Seq[ParquetClass] = (0 until 100).map(ParquetClass)

  def avroSchema: Schema = new Schema.Parser().parse("""
      |{"type":"record",
      |"name":"ParquetClass",
      |"namespace":"com.spotify.ratatool.samplers.ParquetTestData",
      |"fields":[{"name":"id","type":"int"}]}""".stripMargin)

  def writeTestData(avroPath: String, typedPath: String, numShards: Int = 1): Unit = {
    implicit val grCoder: Coder[GenericRecord] = Coder.avroGenericRecordCoder(avroSchema)

    val sc = ScioContext()

    // Write typed Parquet records
    sc.parallelize(ParquetTypedData)
      .saveAsTypedParquetFile(typedPath, numShards = numShards)

    // Write avro Parquet records
    sc.parallelize(ParquetAvroData)
      .saveAsParquetAvroFile(avroPath, avroSchema, numShards = numShards)

    sc.run()

    List(avroPath, typedPath).foreach { p =>
      new File(p).listFiles().foreach(_.deleteOnExit())
    }
  }
}

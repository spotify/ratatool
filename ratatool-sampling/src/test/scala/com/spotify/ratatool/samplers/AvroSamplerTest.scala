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

import java.io.File
import java.nio.file.{Files, Paths}

import com.spotify.ratatool.Schemas
import com.spotify.ratatool.scalacheck._
import com.spotify.ratatool.io.AvroIO
import org.apache.hadoop.fs.Path
import org.scalacheck.Gen
import org.scalatest._

class AvroSamplerTest extends FlatSpec with Matchers with BeforeAndAfterAllConfigMap {

  val schema = Schemas.avroSchema
  val data1 = Gen.listOfN(40000, genericRecordOf(schema)).sample.get
  val data2 = Gen.listOfN(10000, genericRecordOf(schema)).sample.get
  val dir = Files.createTempDirectory("ratatool-")
  val file1 = new File(dir.toString, "part-00000.avro")
  val file2 = new File(dir.toString, "part-00001.avro")
  val dirWildcard = new File(dir.toString, "*.avro")

  override protected def beforeAll(configMap: ConfigMap): Unit = {
    AvroIO.writeToFile(data1, schema, file1)
    AvroIO.writeToFile(data2, schema, file2)

    dir.toFile.deleteOnExit()
    file1.deleteOnExit()
    file2.deleteOnExit()
  }

  "AvroSampler" should "support single file in head mode" in {
    val result = new AvroSampler(file1.getAbsolutePath).sample(10, head = true)
    result.size shouldBe 10
    result should equal (data1.take(10))
  }

  it should "support single file in random mode" in {
    val result = new AvroSampler(file1.getAbsolutePath).sample(10, head = false)
    result.size shouldBe 10
    result.forall(data1.contains(_)) shouldBe true
  }

  it should "support multiple files in head mode" in {
    val result = new AvroSampler(dirWildcard.getAbsolutePath).sample(10, head = true)
    result.size shouldBe 10
    result should equal (data1.take(10))
  }

  it should "support multiple files in random mode" in {
    val result = new AvroSampler(dirWildcard.getAbsolutePath).sample(10, head = false)
    result.size shouldBe 10
    result.exists(data1.contains(_)) shouldBe true
    result.exists(data2.contains(_)) shouldBe true
    result.forall(data1.contains(_)) shouldBe false
    result.forall(data2.contains(_)) shouldBe false
    data1.count(result.contains(_)) should be > data2.count(result.contains(_))
  }

}

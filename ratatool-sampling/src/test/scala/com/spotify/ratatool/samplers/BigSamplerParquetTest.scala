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

import com.spotify.ratatool.io.{ParquetIO, ParquetTestData}
import com.spotify.ratatool.samplers.util.{Approximate, MurmurHash, StratifiedDistribution}
import com.spotify.scio.ScioContext
import com.spotify.scio.testing.PipelineSpec
import org.scalatest.BeforeAndAfterAll

import java.io.File

class BigSamplerParquetTest extends PipelineSpec with BeforeAndAfterAll {
  private lazy val (typedOut, avroOut) =
    (ParquetTestData.createTempDir("typed"), ParquetTestData.createTempDir("avro"))

  override protected def beforeAll(): Unit =
    ParquetTestData.writeTestData(avroPath = avroOut, typedPath = typedOut, numShards = 2)

  "BigSamplerParquet" should "convert typed to GenericRecords and defer to BigSamplerAvro" in {
    val output = s"${ParquetTestData.createTempDir("output")}"
    BigSamplerParquet.sample(
      ScioContext(),
      s"$typedOut/*.parquet",
      output,
      List("id"),
      0.5,
      None,
      MurmurHash,
      Some(StratifiedDistribution),
      Seq(),
      Approximate,
      1e6.toInt
    )

    // Read all shards of output
    val sampledRecords = new File(output).listFiles().map(ParquetIO.readFromFile).toSeq.flatten
    sampledRecords.size should (be <= 65 and be >= 35)
    all(sampledRecords.map(_.get("id").asInstanceOf[Int])) should (be < 100 and be >= 0)
  }
}

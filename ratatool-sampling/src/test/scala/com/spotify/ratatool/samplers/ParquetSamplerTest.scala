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

import com.spotify.ratatool.io.ParquetTestData
import org.apache.avro.generic.GenericRecord
import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ParquetSamplerTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private lazy val (typedOut, avroOut) =
    (ParquetTestData.createTempDir("typed"), ParquetTestData.createTempDir("avro"))

  override protected def beforeAll(): Unit =
    ParquetTestData.writeTestData(avroPath = avroOut, typedPath = typedOut, numShards = 2)

  private val BeBetween0And100 = be < 100 and be >= 0
  private val GetId: GenericRecord => Int = _.get("id").asInstanceOf[Int]

  "ParquetSampler" should "sample typed parquet records from wildcard pattern" in {
    testSampler(new ParquetSampler(s"$typedOut/*.parquet"), head = true)
    testSampler(new ParquetSampler(s"$typedOut/*.parquet"), head = false)
  }

  it should "sample typed parquet records from non-wildcard pattern" in {
    testSampler(new ParquetSampler(s"$typedOut/part-00000-of-00002.parquet"), head = false)
    testSampler(new ParquetSampler(s"$typedOut/part-00000-of-00002.parquet"), head = true)
  }

  it should "sample parquet-avro records from wildcard pattern" in {
    testSampler(new ParquetSampler(s"$avroOut/*.parquet"), head = true)
    testSampler(new ParquetSampler(s"$avroOut/*.parquet"), head = false)
  }

  it should "sample parquet-avro records from non-wildcard pattern" in {
    testSampler(new ParquetSampler(s"$avroOut/part-00000-of-00002.parquet"), head = false)
    testSampler(new ParquetSampler(s"$avroOut/part-00000-of-00002.parquet"), head = true)
  }

  private def testSampler(parquetSampler: ParquetSampler, head: Boolean): Unit = {
    val sampled = parquetSampler.sample(10, head = head)
    sampled should have size 10
    all(sampled.map(GetId)) should BeBetween0And100
  }
}

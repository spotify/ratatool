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

import com.spotify.ratatool.io.{FileStorage, ParquetIO}
import com.spotify.ratatool.samplers.util._
import com.spotify.scio.ScioContext
import com.spotify.scio.avro._
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.{ClosedTap, MaterializeTap}
import com.spotify.scio.parquet.avro._
import org.apache.avro.generic._
import org.slf4j.LoggerFactory

private[samplers] object BigSamplerParquet {
  private val log = LoggerFactory.getLogger(BigSamplerParquet.getClass)

  private[samplers] def sample(
    sc: ScioContext,
    input: String,
    output: String,
    fields: Seq[String],
    fraction: Double,
    seed: Option[Int],
    hashAlgorithm: HashAlgorithm,
    distribution: Option[SampleDistribution],
    distributionFields: Seq[String],
    precision: Precision,
    maxKeySize: Int,
    byteEncoding: ByteEncoding = RawEncoding
  ): ClosedTap[GenericRecord] = {
    val schema = ParquetIO.getAvroSchemaFromFile(input)
    val outputParts = if (output.endsWith("/")) output + "part*" else output + "/part*"

    implicit val coder: Coder[GenericRecord] = avroGenericRecordCoder(schema)
    if (FileStorage(outputParts).isDone) {
      log.info(s"Reuse previous sample at $outputParts")
      ClosedTap(MaterializeTap[GenericRecord](outputParts, sc))
    } else {
      log.info(s"Will sample from: $input, output will be $output")

      val coll = sc.parquetAvroFile[GenericRecord](input, schema).map(identity)

      val sampledCollection = sampleAvro[GenericRecord](
        coll,
        fraction,
        schema,
        fields,
        seed,
        hashAlgorithm,
        distribution,
        distributionFields,
        precision,
        maxKeySize,
        byteEncoding
      )

      val r = sampledCollection.saveAsParquetAvroFile(output, schema = schema)
      sc.run().waitUntilDone()
      r
    }
  }
}

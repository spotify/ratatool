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

import com.spotify.ratatool.io.ParquetIO
import com.spotify.scio.parquet.BeamInputFile
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.io.fs.ResourceId
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import org.apache.parquet.avro.AvroParquetReader
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._
import scala.util.Random

/** Sampler for Parquet files. */
class ParquetSampler(
  path: String,
  protected val seed: Option[Long] = None,
  protected val conf: Option[PipelineOptions] = None
) extends Sampler[GenericRecord] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[ParquetSampler])

  override def sample(n: Long, head: Boolean): Seq[GenericRecord] = {
    require(n > 0, "n must be > 0")
    logger.info("Taking a sample of {} from Parquet {}", n, path)

    FileSystems.setDefaultPipelineOptions(conf.getOrElse(PipelineOptionsFactory.create()))
    val matches = FileSystems.`match`(path).metadata().asScala
    if (!FileSystems.hasGlobWildcard(path)) {
      val resource = matches.head.resourceId()
      new ParquetFileSampler(resource, seed).sample(n, head)
    } else {
      if (head) {
        val resources = matches
          .map(_.resourceId())
          .sortBy(_.toString)
        // read from the start
        val result = ListBuffer.empty[GenericRecord]
        val iter = resources.toIterator
        while (result.size < n && iter.hasNext) {
          result.appendAll(new ParquetFileSampler(iter.next()).sample(n, head))
        }
        result.toList
      } else {
        val tups = matches
          .map(md => (md.resourceId(), md.sizeBytes()))
          .sortBy(_._1.toString)
          .toArray
        // randomly sample from shards
        val sizes = tups.map(_._2)
        val resources = tups.map(_._1)
        val samples = scaleWeights(sizes, n)
        val futures = resources
          .zip(samples)
          .map { case (r, s) =>
            Future(new ParquetFileSampler(r).sample(s, head))
          }
          .toSeq
        Await.result(Future.sequence(futures), Duration.Inf).flatten
      }
    }
  }

  // Scale weights so that they sum up to n
  private def scaleWeights(weights: Array[Long], n: Long): Array[Long] = {
    val sum = weights.sum
    require(sum > n, "sum of weights must be > n")
    val result = weights.map(x => (x.toDouble / sum * n).toLong)

    val delta = n - result.sum // delta due to rounding error
    var i = 0
    val dim = weights.length
    while (i < delta) {
      // randomly distribute delta
      result(Random.nextInt(dim)) += 1
      i += 1
    }
    result
  }

}

private class ParquetFileSampler(r: ResourceId, protected val seed: Option[Long] = None)
    extends Sampler[GenericRecord] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[ParquetFileSampler])

  override def sample(n: Long, head: Boolean): Seq[GenericRecord] = {
    require(n > 0, "n must be > 0")

    val avroSchema = ParquetIO.getAvroSchemaFromFile(r.toString)
    logger.debug("Converted Parquet to Avro schema: {}", avroSchema)

    val jobConfig = ParquetIO.genericRecordReadConfig(avroSchema, r.toString)

    val reader = AvroParquetReader
      .builder[GenericRecord](BeamInputFile.of(r))
      .withConf(jobConfig)
      .build()

    val result = ArrayBuffer.empty[GenericRecord]
    if (head) {
      // read from the start
      var next = reader.read()
      while (result.size < n && next != null) {
        result.append(next)
        next = reader.read()
      }
    } else {
      // Reservoir sample imperative way: Fill result with first n elements
      var next = reader.read()
      while (result.size < n && next != null) {
        result.append(next)
        next = reader.read()
      }
      // Then randomly select from all other elements in the stream
      var index = n
      while (next != null) {
        val loc = nextLong(index + 1)
        if (loc < n) {
          result(loc.toInt) = next
        }
        next = reader.read()
        index += 1
      }
    }
    reader.close()
    result.toList
  }
}

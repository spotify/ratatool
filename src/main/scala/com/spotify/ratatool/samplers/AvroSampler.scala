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

import com.spotify.ratatool.GcsConfiguration
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.hadoop.fs._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.{ListBuffer, Set => MSet}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Random

class AvroSampler(path: Path, protected val seed: Option[Long] = None)
  extends Sampler[GenericRecord] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[AvroSampler])

  override def sample(n: Long, head: Boolean): Seq[GenericRecord] = {
    require(n > 0, "n must be > 0")
    logger.info("Taking a sample of {} from Avro {}", n, path)

    val fs = FileSystem.get(path.toUri, GcsConfiguration.get())
    if (fs.isFile(path)) {
      new AvroFileSampler(path, seed).sample(n, head)
    } else {
      val filter = new PathFilter {
        override def accept(path: Path): Boolean = path.getName.endsWith(".avro")
      }
      val statuses = fs.listStatus(path, filter)
      val paths = statuses.map(_.getPath)

      if (head) {
        // read from the start
        val result = ListBuffer.empty[GenericRecord]
        val iter = paths.iterator
        while (result.size < n && iter.hasNext) {
          result.appendAll(new AvroFileSampler(iter.next()).sample(n, head))
        }
        result
      } else {
        // randomly sample from shards
        val sizes = statuses.map(_.getLen)
        val samples = scaleWeights(sizes, n)
        val futures = paths.zip(samples).map { case (p, s) =>
          Future(new AvroFileSampler(p).sample(s, head))
        }.toSeq
        Await.result(Future.sequence(futures), Duration.Inf).flatten
      }
    }
  }

  // Scale weights so that they sum up to n
  private def scaleWeights(weights: Array[Long], n: Long): Array[Long] = {
    val sum = weights.sum
    require(sum > n, "sum of weights must be > n")
    val result = weights.map(x => (x.toDouble / sum * n).toLong)

    val delta = n - result.sum  // delta due to rounding error
    var i = 0
    val dim = weights.length
    while (i < delta) {
      // randomly distribute delta
      result(Random.nextInt(dim)) += 1
      i+= 1
    }
    result
  }

}

private class AvroFileSampler(path: Path, protected val seed: Option[Long] = None)
  extends Sampler[GenericRecord] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[AvroFileSampler])

  override def sample(n: Long, head: Boolean): Seq[GenericRecord] = {
    require(n > 0, "n must be > 0")
    logger.debug("Taking a sample of {} from Avro file {}", n, path)

    val input = new AvroFSInput(FileContext.getFileContext(GcsConfiguration.get()), path)
    val datumReader = new GenericDatumReader[GenericRecord]()
    val fileReader = DataFileReader.openReader(input, datumReader)

    val schema = fileReader.getSchema
    logger.debug("Avro schema {}", schema)
    val start = fileReader.tell()
    val end = input.length()
    val range = end - start

    val result = ListBuffer.empty[GenericRecord]
    if (head) {
      // read from the start
      while (result.size < n && fileReader.hasNext) {
        result.append(fileReader.next())
      }
    } else {
      // rejection sampling until n unique samples are obtained
      val positions = MSet.empty[Long]
      var collisions = 0
      while (result.size < n && collisions < 10) {
        // pick a random offset and move to the next sync point
        val off = start + nextLong(range)
        fileReader.sync(off)
        val pos = fileReader.tell()

        // sync position may be sampled already
        if (positions.contains(pos)) {
          collisions += 1
          logger.debug("Sync point collision {} at position {}", collisions, pos)
        } else {
          collisions = 0
          positions.add(pos)
          result.append(fileReader.next())
          logger.debug("New sample sync point at position {}", pos)
        }
      }
    }
    result
  }

}

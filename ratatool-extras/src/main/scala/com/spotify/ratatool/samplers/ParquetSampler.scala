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

import com.spotify.ratatool.io.ParquetIO
import org.apache.avro.generic.GenericRecord
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer

/**
 * Sampler for Parquet files.
 *
 * Records are represented as Avro [[GenericRecord]]. Only head mode is supported.
 */
class ParquetSampler(path: String, protected val seed: Option[Long] = None)
    extends Sampler[GenericRecord] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[ParquetSampler])

  override def sample(n: Long, head: Boolean): Seq[GenericRecord] = {
    require(n > 0, "n must be > 0")
    require(head, "Parquet can only be used with --head")
    logger.info("Taking a sample of {} from Parquet {}", n, path)

    val result = ListBuffer.empty[GenericRecord]
    val iterator = ParquetIO.readFromFile(path)
    while (result.length < n && iterator.hasNext) {
      result.append(iterator.next())
    }
    result.toList
  }

}

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

package com.spotify.ratatool.generators

import java.util.Random

import com.google.cloud.dataflow.sdk.coders.AvroCoder
import com.google.cloud.dataflow.sdk.util.CoderUtils
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecord
import org.apache.avro.{RandomData, Schema}

import scala.reflect.ClassTag

/** Random generator of Avro records. */
object AvroGenerator {

  private val random = new Random

  private val cache: LoadingCache[Class[_], SpecificGenerator[_]] = CacheBuilder
    .newBuilder()
    .build(new CacheLoader[Class[_], SpecificGenerator[_]] {
      override def load(key: Class[_]): SpecificGenerator[_] = new SpecificGenerator(key)
    })

  /** Generate a generic record. */
  def avroOf(schema: Schema): GenericRecord =
    new RandomData(schema, 1).iterator().next().asInstanceOf[GenericRecord]

  /** Generate a specific record. */
  def avroOf[T <: SpecificRecord : ClassTag]: T =
    cache.get(implicitly[ClassTag[T]].runtimeClass).asInstanceOf[SpecificGenerator[T]]()

  private class SpecificGenerator[T](cls: Class[_]) {
    private val specificCoder = AvroCoder.of(cls).asInstanceOf[AvroCoder[T]]
    private val genericCoder = AvroCoder.of(specificCoder.getSchema)
    def apply(): T = {
      val genericRecord = avroOf(specificCoder.getSchema)
      val bytes = CoderUtils.encodeToByteArray(genericCoder, genericRecord)
      CoderUtils.decodeFromByteArray(specificCoder, bytes)
    }
  }

}

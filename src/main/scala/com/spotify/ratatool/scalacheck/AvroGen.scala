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

package com.spotify.ratatool.scalacheck

import java.util.Random

import com.google.cloud.dataflow.sdk.coders.AvroCoder
import com.google.cloud.dataflow.sdk.util.CoderUtils
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecord
import org.apache.avro.{RandomData, Schema}
import org.scalacheck._

import scala.reflect.ClassTag

object AvroGen {

  def avroOf(schema: Schema): Gen[GenericRecord] =
    Gen.const(new Random).map(RandomData.generate(schema, _, 0).asInstanceOf[GenericRecord])

  def avroOf[T <: SpecificRecord : ClassTag]: Gen[T] = {
    val cls = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
    val sCoder = AvroCoder.of(cls)
    val gCoder = AvroCoder.of(sCoder.getSchema)
    avroOf(sCoder.getSchema).map { r =>
      val b = CoderUtils.encodeToByteArray(gCoder, r)
      CoderUtils.decodeFromByteArray(sCoder, b)
    }
  }

}

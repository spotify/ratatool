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

import com.spotify.ratatool.generators.AvroGenerator
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecord
import org.scalacheck._

import scala.reflect.ClassTag

object AvroGen {

  def avroOf(schema: Schema): Gen[GenericRecord] =
    Gen.const(0).map(_ => AvroGenerator.avroOf(schema))

  def avroOf[T <: SpecificRecord : ClassTag]: Gen[T] =
    Gen.const(0).map(_ => AvroGenerator.avroOf[T])

}

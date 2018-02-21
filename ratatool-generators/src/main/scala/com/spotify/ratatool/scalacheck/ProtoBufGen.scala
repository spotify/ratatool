/*
 * Copyright 2018 Spotify AB.
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

import com.google.protobuf.AbstractMessage
import com.spotify.ratatool.generators.ProtoBufGenerator
import org.scalacheck._

import scala.reflect.ClassTag

object ProtoBufGen {

  /** ScalaCheck generator of ProtoBuf records. */
  def protoBufOf[T <: AbstractMessage : ClassTag]: Gen[T] =
    Gen.const(0).map(_ => ProtoBufGenerator.protoBufOf[T])

}

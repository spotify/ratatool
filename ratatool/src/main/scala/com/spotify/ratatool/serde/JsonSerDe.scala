/*
 * Copyright 2017 Spotify AB.
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

package com.spotify.ratatool.serde

import com.google.cloud.dataflow.sdk.util.Transport

import scala.util.{Failure, Success, Try}

private[ratatool] object JsonSerDe {
  private val jsonFactory = Transport.getJsonFactory

  def toJsonString(item: Any): String = {
    if (item == null) {
      null
    } else {
      Try(jsonFactory.toString(item)) match {
        case Success(s) => s
        case Failure(e) =>
          throw new RuntimeException(String.format("Cannot serialize %s to a JSON string.",
            item.getClass.getSimpleName), e)
      }
    }
  }

  def fromJsonString[T](json: String, clazz: Class[T]): T = {
    if (json == null) {
      null.asInstanceOf[T]
    } else {
      Try(jsonFactory.fromString(json, clazz)) match {
        case Success(t) => t
        case Failure(e) =>
          throw new RuntimeException(
            String.format("Cannot deserialize %s from a JSON string: %s.", clazz, json), e)
      }
    }
  }

}

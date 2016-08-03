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

import scala.util.Random

trait Sampler[T] {
  protected val seed: Option[Long]
  protected val random: Random = seed match {
    case Some(s) => new Random(s)
    case None => new Random()
  }

  protected def nextLong(n: Long): Long = {
    require(n > 0, "n must be positive")
    var bits: Long = 0
    var value: Long = 0
    do {
      bits = random.nextLong() << 1 >>> 1
      value = bits % n
    } while (bits - value + (n - 1) < 0)
    value
  }

  def sample(n: Long, head: Boolean): Seq[T]
}

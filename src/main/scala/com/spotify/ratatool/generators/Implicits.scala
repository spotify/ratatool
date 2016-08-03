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

object Implicits {

  implicit class RichRandom(random: Random) {

    def nextLong(n: Long): Long = {
      require(n > 0, "n must be positive")
      var bits: Long = 0
      var value: Long = 0
      do {
        bits = random.nextLong() << 1 >>> 1
        value = bits % n
      } while (bits - value + (n - 1) < 0)
      value
    }

    def nextString(maxLength: Int): String = {
      val len = math.max(2, random.nextInt(maxLength))
      val chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
      (1 to len).map(_ => chars.charAt(random.nextInt(chars.length))).mkString
    }

    def nextBytes(maxLength: Int): Array[Byte] = {
      val len = math.max(2, random.nextInt(maxLength))
      val bytes = new Array[Byte](len)
      random.nextBytes(bytes)
      bytes
    }

  }

}

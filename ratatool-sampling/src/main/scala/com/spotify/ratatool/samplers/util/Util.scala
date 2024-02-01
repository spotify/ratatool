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

package com.spotify.ratatool.samplers.util

import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.{Hasher, Hashing}

trait SampleDistribution
case object StratifiedDistribution extends SampleDistribution
case object UniformDistribution extends SampleDistribution

object SampleDistribution {
  def fromString(s: String): SampleDistribution = {
    if (s == "stratified") {
      StratifiedDistribution
    } else if (s == "uniform") {
      UniformDistribution
    } else {
      throw new IllegalArgumentException(s"Invalid distribution $s")
    }
  }
}

trait Determinism
case object NonDeterministic extends Determinism
case object Deterministic extends Determinism

object Determinism {
  def fromSeq(l: Seq[_]): Determinism = {
    if (l == Seq()) {
      NonDeterministic
    } else {
      Deterministic
    }
  }
}

trait Precision
case object Approximate extends Precision
case object Exact extends Precision

object Precision {
  def fromBoolean(exact: Boolean): Precision = {
    if (exact) {
      Exact
    } else {
      Approximate
    }
  }
}

trait ByteEncoding
case object RawEncoding extends ByteEncoding
case object HexEncoding extends ByteEncoding
case object Base64Encoding extends ByteEncoding

object ByteEncoding {
  def fromString(s: String): ByteEncoding = {
    if (s == "raw") {
      RawEncoding
    } else if (s == "hex") {
      HexEncoding
    } else if (s == "base64") {
      Base64Encoding
    } else {
      throw new IllegalArgumentException(s"Invalid byte encoding $s")
    }
  }
}

sealed trait HashAlgorithm {
  def hashFn(seed: Option[Int]): Hasher
}

case object MurmurHash extends HashAlgorithm {
  def hashFn(seed: Option[Int]): Hasher =
    Hashing.murmur3_128(seed.getOrElse(System.currentTimeMillis().toInt)).newHasher()
}
case object FarmHash extends HashAlgorithm {
  def hashFn(seed: Option[Int]): Hasher = seed match {
    case Some(s) => Hashing.farmHashFingerprint64().newHasher().putInt(s)
    case _       => Hashing.farmHashFingerprint64().newHasher()
  }
}

object HashAlgorithm {
  def fromString(s: String): HashAlgorithm = {
    if (s == "murmur") {
      MurmurHash
    } else if (s == "farm") {
      FarmHash
    } else {
      throw new IllegalArgumentException(s"Invalid hashing function $s")
    }
  }
}

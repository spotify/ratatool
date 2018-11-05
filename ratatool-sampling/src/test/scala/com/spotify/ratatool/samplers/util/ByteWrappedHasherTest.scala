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

import com.google.common.hash.{Funnel, PrimitiveSink}
import com.google.common.io.BaseEncoding
import com.spotify.ratatool.samplers.BigSampler
import org.scalacheck.Prop.forAll
import org.scalacheck.Properties
import org.scalatest.{FlatSpec, Matchers}

object ByteWrappedHasherTest extends Properties("ByteWrappedHasher") {
  private val testSeed = Some(42)
  private def newHasher() = BigSampler.hashFun(seed = testSeed)

  property("wrapped hasher produces the same hash as original hasher for ints") = forAll {
    i: Int =>
    val hasher = newHasher
    val wrappedHasher =
      ByteHasher.wrap(newHasher, RawEncoding, BigSampler.utf8Charset)

    hasher.putInt(i)
    wrappedHasher.putInt(i)

    hasher.hash == wrappedHasher.hash
  }

  property("wrapped hasher produces the same hash as original hasher for shorts") = forAll {
    s: Short =>
    val hasher = newHasher
    val wrappedHasher =
      ByteHasher.wrap(newHasher, HexEncoding, BigSampler.utf8Charset)

    hasher.putShort(s)
    wrappedHasher.putShort(s)

    hasher.hash == wrappedHasher.hash
  }

  property("wrapped hasher produces the same hash as original hasher for longs") = forAll {
    l: Long =>
      val hasher = newHasher
      val wrappedHasher =
        ByteHasher.wrap(newHasher, HexEncoding, BigSampler.utf8Charset)

      hasher.putLong(l)
      wrappedHasher.putLong(l)

      hasher.hash == wrappedHasher.hash
  }

  property("wrapped hasher produces the same hash as original hasher for floats") = forAll {
    f: Float =>
      val hasher = newHasher
      val wrappedHasher =
        ByteHasher.wrap(newHasher, HexEncoding, BigSampler.utf8Charset)

      hasher.putFloat(f)
      wrappedHasher.putFloat(f)

      hasher.hash == wrappedHasher.hash
  }

  property("wrapped hasher produces the same hash as original hasher for doubles") = forAll {
    d: Float =>
      val hasher = newHasher
      val wrappedHasher =
        ByteHasher.wrap(newHasher, HexEncoding, BigSampler.utf8Charset)

      hasher.putDouble(d)
      wrappedHasher.putDouble(d)

      hasher.hash == wrappedHasher.hash
  }

  property("wrapped hasher produces the same hash as original hasher for booleans") = forAll {
    b: Boolean =>
      val hasher = newHasher
      val wrappedHasher =
        ByteHasher.wrap(newHasher, HexEncoding, BigSampler.utf8Charset)

      hasher.putBoolean(b)
      wrappedHasher.putBoolean(b)

      hasher.hash == wrappedHasher.hash
  }

  property("wrapped hasher produces the same hash as original hasher for chars") = forAll {
    c: Char =>
      val hasher = newHasher
      val wrappedHasher =
        ByteHasher.wrap(newHasher, HexEncoding, BigSampler.utf8Charset)

      hasher.putChar(c)
      wrappedHasher.putChar(c)

      hasher.hash == wrappedHasher.hash
  }

  property("wrapped hasher produces the same hash as original hasher for char sequences") = forAll {
    s: String =>
      val hasher = newHasher
      val wrappedHasher =
        ByteHasher.wrap(newHasher, HexEncoding, BigSampler.utf8Charset)

      hasher.putUnencodedChars(s)
      wrappedHasher.putUnencodedChars(s)

      hasher.hash == wrappedHasher.hash
  }

  property("wrapped hasher produces the same hash as original hasher for strings") = forAll {
    s: String =>
      val hasher = newHasher
      val wrappedHasher =
        ByteHasher.wrap(newHasher, HexEncoding, BigSampler.utf8Charset)

      hasher.putString(s, BigSampler.utf8Charset)
      wrappedHasher.putString(s, BigSampler.utf8Charset)

      hasher.hash == wrappedHasher.hash
  }

  object StringFunnel extends Funnel[String] {
    override def funnel(from: String, into: PrimitiveSink): Unit = {
      into.putString(from, BigSampler.utf8Charset)
    }
  }

  property("wrapped hasher produces the same hash as original hasher for objects") = forAll {
    s: String =>
      val hasher = newHasher
      val wrappedHasher =
        ByteHasher.wrap(newHasher, HexEncoding, BigSampler.utf8Charset)

      hasher.putObject(s, StringFunnel)
      wrappedHasher.putObject(s, StringFunnel)

      hasher.hash == wrappedHasher.hash
  }

  property("wrapped hex hasher correctly encodes bytes as hex") = forAll {
    ba: Array[Byte] =>
      val hasher1 =
        ByteHasher.wrap(newHasher, HexEncoding, BigSampler.utf8Charset)
      val hasher2 =
        ByteHasher.wrap(newHasher, HexEncoding, BigSampler.utf8Charset)

      val s = BaseEncoding.base16().lowerCase.encode(ba)

      hasher1.putBytes(ba)
      hasher2.putString(s, BigSampler.utf8Charset)
      assert(hasher1.hash == hasher2.hash)

      if(ba.length >= 1) {
        hasher1.putBytes(ba, 0, 1)
        hasher2.putString(s.slice(0, 2), BigSampler.utf8Charset)
        assert(hasher1.hash == hasher2.hash)

        hasher1.putByte(ba(0))
        hasher2.putString(s.slice(0, 2), BigSampler.utf8Charset)
      }

      hasher1.hash == hasher2.hash
  }
}

class ByteHasherEqualsTest extends FlatSpec with Matchers {
  private val testSeed = Some(42)
  private def newHasher() = BigSampler.hashFun(seed = testSeed)

  "WrappedByteHasher" should "not alter the hasher using the raw encoding" in {
    val hasher = newHasher
    val wrappedHasher = ByteHasher.wrap(hasher, RawEncoding, BigSampler.utf8Charset)
    assert(hasher.equals(wrappedHasher))
  }
}

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

import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.{Funnel, PrimitiveSink}
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.BaseEncoding
import com.spotify.ratatool.samplers.BigSampler
import org.scalacheck.Prop.forAll
import org.scalacheck.Properties
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class ByteWrappedHasherTest extends AnyFlatSpec with Matchers with ScalaCheckDrivenPropertyChecks {
  private val testSeed = Some(42)
  private def newHasher() = BigSampler.hashFun(seed = testSeed)

  "ByteWrappedHasher" should "produce the same hash as original hasher for ints" in {
    forAll { i: Int =>
      val hasher = newHasher
      val wrappedHasher =
        ByteHasher.wrap(newHasher, RawEncoding, BigSampler.utf8Charset)

      hasher.putInt(i)
      wrappedHasher.putInt(i)

      hasher.hash shouldBe wrappedHasher.hash
    }
  }

  it should "produce the same hash as original hasher for shorts" in {
    forAll { s: Short =>
      val hasher = newHasher
      val wrappedHasher =
        ByteHasher.wrap(newHasher, HexEncoding, BigSampler.utf8Charset)

      hasher.putShort(s)
      wrappedHasher.putShort(s)

      hasher.hash shouldBe wrappedHasher.hash
    }
  }

  it should "produce the same hash as original hasher for longs" in {
    forAll { l: Long =>
      val hasher = newHasher
      val wrappedHasher =
        ByteHasher.wrap(newHasher, HexEncoding, BigSampler.utf8Charset)

      hasher.putLong(l)
      wrappedHasher.putLong(l)

      hasher.hash shouldBe wrappedHasher.hash
    }
  }

  it should "produce the same hash as original hasher for floats" in {
    forAll { f: Float =>
      val hasher = newHasher
      val wrappedHasher =
        ByteHasher.wrap(newHasher, HexEncoding, BigSampler.utf8Charset)

      hasher.putFloat(f)
      wrappedHasher.putFloat(f)

      hasher.hash shouldBe wrappedHasher.hash
    }
  }

  it should "produce the same hash as original hasher for doubles" in {
    forAll { d: Float =>
      val hasher = newHasher
      val wrappedHasher =
        ByteHasher.wrap(newHasher, HexEncoding, BigSampler.utf8Charset)

      hasher.putDouble(d)
      wrappedHasher.putDouble(d)

      hasher.hash shouldBe wrappedHasher.hash
    }
  }

  it should "produce the same hash as original hasher for booleans" in {
    forAll { b: Boolean =>
      val hasher = newHasher
      val wrappedHasher =
        ByteHasher.wrap(newHasher, HexEncoding, BigSampler.utf8Charset)

      hasher.putBoolean(b)
      wrappedHasher.putBoolean(b)

      hasher.hash shouldBe wrappedHasher.hash
    }
  }

  it should "produce the same hash as original hasher for chars" in {
    forAll { c: Char =>
      val hasher = newHasher
      val wrappedHasher =
        ByteHasher.wrap(newHasher, HexEncoding, BigSampler.utf8Charset)

      hasher.putChar(c)
      wrappedHasher.putChar(c)

      hasher.hash shouldBe wrappedHasher.hash
    }
  }

  it should "produce the same hash as original hasher for char sequences" in {
    forAll { s: String =>
      val hasher = newHasher
      val wrappedHasher =
        ByteHasher.wrap(newHasher, HexEncoding, BigSampler.utf8Charset)

      hasher.putUnencodedChars(s)
      wrappedHasher.putUnencodedChars(s)

      hasher.hash shouldBe wrappedHasher.hash
    }
  }

  it should "produce the same hash as original hasher for strings" in {
    forAll { s: String =>
      val hasher = newHasher
      val wrappedHasher =
        ByteHasher.wrap(newHasher, HexEncoding, BigSampler.utf8Charset)

      hasher.putString(s, BigSampler.utf8Charset)
      wrappedHasher.putString(s, BigSampler.utf8Charset)

      hasher.hash shouldBe wrappedHasher.hash
    }
  }

  object StringFunnel extends Funnel[String] {
    override def funnel(from: String, into: PrimitiveSink): Unit =
      into.putString(from, BigSampler.utf8Charset)
  }

  it should "produce the same hash as original hasher for objects" in {
    forAll { s: String =>
      val hasher = newHasher
      val wrappedHasher =
        ByteHasher.wrap(newHasher, HexEncoding, BigSampler.utf8Charset)

      hasher.putObject(s, StringFunnel)
      wrappedHasher.putObject(s, StringFunnel)

      hasher.hash shouldBe wrappedHasher.hash
    }
  }

  it should "correctly encodes bytes as hex" in {
    forAll { ba: Array[Byte] =>
      val hasher1 =
        ByteHasher.wrap(newHasher, HexEncoding, BigSampler.utf8Charset)
      val hasher2 =
        ByteHasher.wrap(newHasher, HexEncoding, BigSampler.utf8Charset)

      val s = BaseEncoding.base16().lowerCase.encode(ba)

      hasher1.putBytes(ba)
      hasher2.putString(s, BigSampler.utf8Charset)
      assert(hasher1.hash == hasher2.hash)

      if (ba.length >= 1) {
        hasher1.putBytes(ba, 0, 1)
        hasher2.putString(s.slice(0, 2), BigSampler.utf8Charset)
        assert(hasher1.hash == hasher2.hash)

        hasher1.putByte(ba(0))
        hasher2.putString(s.slice(0, 2), BigSampler.utf8Charset)
      }

      hasher1.hash shouldBe hasher2.hash
    }
  }

  it should "not alter the hasher using the raw encoding" in {
    val hasher = newHasher()
    val wrappedHasher = ByteHasher.wrap(hasher, RawEncoding, BigSampler.utf8Charset)
    assert(hasher.equals(wrappedHasher))
  }
}

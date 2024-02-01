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
package com.spotify.ratatool.samplers.util

import java.nio.charset.Charset

import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.{Funnel, HashCode, Hasher}
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.BaseEncoding

object ByteHasher {
  def wrap(hasher: Hasher, be: ByteEncoding, charset: Charset): Hasher = {
    be match {
      case RawEncoding => hasher
      case HexEncoding =>
        new ByteHasher(hasher, BaseEncoding.base16.lowerCase, charset)
      case Base64Encoding =>
        new ByteHasher(hasher, BaseEncoding.base64, charset)
    }
  }
}

class ByteHasher(hasher: Hasher, encoding: BaseEncoding, charset: Charset) extends Hasher {
  override def putByte(b: Byte): Hasher =
    hasher.putString(encoding.encode(Array(b)), charset)

  override def putBytes(bytes: Array[Byte]): Hasher =
    hasher.putString(encoding.encode(bytes), charset)

  override def putBytes(bytes: Array[Byte], off: Int, len: Int): Hasher =
    hasher.putString(encoding.encode(bytes, off, len), charset)

  override def putBytes(bytes: java.nio.ByteBuffer): Hasher =
    hasher.putBytes(bytes)

  override def putShort(s: Short): Hasher = hasher.putShort(s)

  override def putInt(i: Int): Hasher = hasher.putInt(i)

  override def putLong(l: Long): Hasher = hasher.putLong(l)

  override def putFloat(f: Float): Hasher = hasher.putFloat(f)

  override def putDouble(d: Double): Hasher = hasher.putDouble(d)

  override def putBoolean(b: Boolean): Hasher = hasher.putBoolean(b)

  override def putChar(c: Char): Hasher = hasher.putChar(c)

  override def putUnencodedChars(cs: CharSequence): Hasher = hasher.putUnencodedChars(cs)

  override def putString(cs: CharSequence, charset: Charset): Hasher = hasher.putString(cs, charset)

  override def putObject[T](instance: T, funnel: Funnel[_ >: T]): Hasher =
    hasher.putObject(instance, funnel)

  override def hash(): HashCode = hasher.hash()
}

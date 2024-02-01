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

package com.spotify.ratatool.samplers

import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.Hasher
import com.google.protobuf.{AbstractMessage, ByteString}
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

private[samplers] object BigSamplerProto {
  private val log = LoggerFactory.getLogger(BigSamplerProto.getClass)

  /**
   * Builds a key function per record Sets do not have deterministic ordering so we return a sorted
   * list
   */
  private[samplers] def buildKey(
    distributionFields: Seq[String]
  )(m: AbstractMessage): List[String] = {
    distributionFields
      .map(f => getProtobufField(m, f))
      .toSet
      .map { x: Any =>
        // can't call toString on null
        if (x == null) {
          "null"
        } else {
          x.toString
        }
      }
      .toList
      .sorted
  }

  private[samplers] def hashProtobufField[T <: AbstractMessage: ClassTag](
    m: T,
    fieldStr: String,
    hasher: Hasher
  ): Hasher = {
    val subfields = fieldStr.split(BigSampler.fieldSep)
    val field = Option(m.getDescriptorForType.findFieldByName(subfields.head)).getOrElse {
      throw new NoSuchElementException(s"Can't find field $fieldStr in protobuf schema")
    }
    val v = m.getField(field)
    if (v == null) {
      log.debug(
        s"Field `${field.getFullName}` of type ${field.getType} is null - won't account" +
          s" for hash"
      )
      hasher
    } else {
      field.getJavaType match {
        case JavaType.MESSAGE =>
          hashProtobufField(v.asInstanceOf[AbstractMessage], subfields.tail.mkString("."), hasher)
        case JavaType.INT     => hasher.putLong(v.asInstanceOf[Int].toLong)
        case JavaType.LONG    => hasher.putLong(v.asInstanceOf[Long])
        case JavaType.FLOAT   => hasher.putFloat(v.asInstanceOf[Float])
        case JavaType.DOUBLE  => hasher.putDouble(v.asInstanceOf[Double])
        case JavaType.BOOLEAN => hasher.putBoolean(v.asInstanceOf[Boolean])
        case JavaType.STRING =>
          hasher.putString(v.asInstanceOf[CharSequence], BigSampler.utf8Charset)
        case JavaType.BYTE_STRING => hasher.putBytes(v.asInstanceOf[ByteString].toByteArray)
        case JavaType.ENUM => hasher.putString(v.asInstanceOf[Enum[_]].name, BigSampler.utf8Charset)
        // Array, Union
      }
    }
  }

  private[samplers] def getProtobufField[T <: AbstractMessage: ClassTag](
    m: T,
    fieldStr: String
  ): Any = {
    val subfields = fieldStr.split(BigSampler.fieldSep)
    val field = Option(m.getDescriptorForType.findFieldByName(subfields.head)).getOrElse {
      throw new NoSuchElementException(s"Can't find field $fieldStr in protobuf schema")
    }
    val v = m.getField(field)
    if (v == null) {
      log.debug(
        s"Field `${field.getFullName}` of type ${field.getType} is null - won't account" +
          s" for key"
      )
    } else {
      field.getJavaType match {
        case JavaType.MESSAGE =>
          getProtobufField(v.asInstanceOf[AbstractMessage], subfields.tail.mkString("."))
        case _ => v
      }
    }
  }
}

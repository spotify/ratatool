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

import java.io.ByteArrayOutputStream
import java.lang.reflect.Method
import java.util.Random

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label
import com.google.protobuf.Descriptors.Descriptor
import com.google.protobuf.Descriptors.FieldDescriptor.Type
import com.google.protobuf.{CodedOutputStream, Descriptors, AbstractMessage}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/** Random generator of ProtoBuf records. */
object ProtoBufGenerator {

  import Implicits._

  private val random = new Random

  private val cache: LoadingCache[Class[_], (Descriptors.Descriptor, Method)] = CacheBuilder
    .newBuilder()
    .build(new CacheLoader[Class[_], (Descriptors.Descriptor, Method)] {
      override def load(key: Class[_]): (Descriptor, Method) = {
        val desc = key.getMethod("getDescriptor").invoke(null).asInstanceOf[Descriptors.Descriptor]
        val parseFn = key.getMethod("parseFrom", classOf[Array[Byte]])
        (desc, parseFn)
      }
    })

  /** Generate a ProtoBuf record. */
  def protoBufOf[T <: AbstractMessage : ClassTag]: T = {
    val (desc, parseFn) = cache.get(implicitly[ClassTag[T]].runtimeClass)
    val bytes = generate(desc)
    parseFn.invoke(null, bytes).asInstanceOf[T]
  }

  private def generate(desc: Descriptors.Descriptor): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val cos = CodedOutputStream.newInstance(baos)
    writeFields(cos, desc)
    cos.flush()
    baos.toByteArray
  }

  private def writeFields(cos: CodedOutputStream, desc: Descriptors.Descriptor): Unit =
    desc.getFields.asScala.foreach(writeField(cos, _))

  // scalastyle:off cyclomatic.complexity
  private def writeField(cos: CodedOutputStream, field: Descriptors.FieldDescriptor): Unit = {
    val id = field.getNumber
    def writeV() = field.getType match {
      case Type.DOUBLE => cos.writeDouble(id, random.nextDouble())
      case Type.FLOAT => cos.writeFloat(id, random.nextFloat())
      case Type.INT32 => cos.writeInt32(id, random.nextInt())
      case Type.INT64 => cos.writeInt64(id, random.nextLong())
      case Type.UINT32 => cos.writeUInt32(id, random.nextInt(Int.MaxValue))
      case Type.UINT64 => cos.writeUInt64(id, random.nextLong(Long.MaxValue))
      case Type.SINT32 => cos.writeSInt32(id, random.nextInt(Int.MaxValue))
      case Type.SINT64 => cos.writeSInt64(id, random.nextLong(Long.MaxValue))
      case Type.FIXED32 => cos.writeFixed32(id, random.nextInt(Int.MaxValue))
      case Type.FIXED64 => cos.writeFixed64(id, random.nextLong(Long.MaxValue))
      case Type.SFIXED32 => cos.writeSFixed32(id, random.nextInt(Int.MaxValue))
      case Type.SFIXED64 => cos.writeSFixed64(id, random.nextLong(Long.MaxValue))
      case Type.BOOL => cos.writeBool(id, random.nextBoolean())
      case Type.STRING => cos.writeString(id, random.nextString(40))
      case Type.ENUM =>
        val enums = field.getEnumType.getValues.asScala
        val enum = enums(random.nextInt(enums.length))
        cos.writeEnum(id, enum.getNumber)
      case Type.BYTES => cos.writeByteArray(id, random.nextBytes(40))
      case Type.MESSAGE => cos.writeByteArray(id, generate(field.getMessageType))
      case t => throw new RuntimeException(s"Unsupported field type $t")
    }
    field.toProto.getLabel match {
      case Label.LABEL_OPTIONAL => if (random.nextBoolean()) writeV()
      case Label.LABEL_REQUIRED => writeV()
      case Label.LABEL_REPEATED =>
        val n = random.nextInt(5) + 2
        (1 to n).foreach(_ => writeV())
    }
  }
  // scalastyle:on cyclomatic.complexity

}

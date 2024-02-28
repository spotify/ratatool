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

import java.io.ByteArrayOutputStream
import java.lang.reflect.Method

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label
import com.google.protobuf.{AbstractMessage, CodedOutputStream, Descriptors}
import com.google.protobuf.Descriptors.{Descriptor, EnumValueDescriptor, FieldDescriptor}
import com.google.protobuf.Descriptors.FieldDescriptor.Type
import org.scalacheck.{Arbitrary, Gen}

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

/**
 * Contains mappings to CodedOutputStream writer functions. Allows creation of all Generators before
 * deriving each generated `Array[Byte]``
 */
private object ProtoBufWriters {
  type PartialWriter = CodedOutputStream => Writer[_]

  /**
   * Curried Writer class to return the Partially constructed class and only specify
   * CodedOutputStream when constructing the `Array[Byte]``
   */
  abstract class Writer[T](id: Int, v: T)(cos: CodedOutputStream) {
    def write(): Unit
  }

  case class DoubleWriter(id: Int, v: Double)(cos: CodedOutputStream) extends Writer(id, v)(cos) {
    override def write(): Unit = cos.writeDouble(id, v)
  }

  case class FloatWriter(id: Int, v: Float)(cos: CodedOutputStream) extends Writer(id, v)(cos) {
    override def write(): Unit = cos.writeFloat(id, v)
  }

  case class Int32Writer(id: Int, v: Int)(cos: CodedOutputStream) extends Writer(id, v)(cos) {
    override def write(): Unit = cos.writeInt32(id, v)
  }

  case class UInt32Writer(id: Int, v: Int)(cos: CodedOutputStream) extends Writer(id, v)(cos) {
    override def write(): Unit = cos.writeUInt32(id, v)
  }

  case class SInt32Writer(id: Int, v: Int)(cos: CodedOutputStream) extends Writer(id, v)(cos) {
    override def write(): Unit = cos.writeSInt32(id, v)
  }

  case class Int64Writer(id: Int, v: Long)(cos: CodedOutputStream) extends Writer(id, v)(cos) {
    override def write(): Unit = cos.writeInt64(id, v)
  }

  case class UInt64Writer(id: Int, v: Long)(cos: CodedOutputStream) extends Writer(id, v)(cos) {
    override def write(): Unit = cos.writeUInt64(id, v)
  }

  case class SInt64Writer(id: Int, v: Long)(cos: CodedOutputStream) extends Writer(id, v)(cos) {
    override def write(): Unit = cos.writeSInt64(id, v)
  }

  case class Fixed32Writer(id: Int, v: Int)(cos: CodedOutputStream) extends Writer(id, v)(cos) {
    override def write(): Unit = cos.writeFixed32(id, v)
  }

  case class SFixed32Writer(id: Int, v: Int)(cos: CodedOutputStream) extends Writer(id, v)(cos) {
    override def write(): Unit = cos.writeSFixed32(id, v)
  }

  case class Fixed64Writer(id: Int, v: Long)(cos: CodedOutputStream) extends Writer(id, v)(cos) {
    override def write(): Unit = cos.writeFixed64(id, v)
  }

  case class SFixed64Writer(id: Int, v: Long)(cos: CodedOutputStream) extends Writer(id, v)(cos) {
    override def write(): Unit = cos.writeSFixed64(id, v)
  }

  case class BoolWriter(id: Int, v: Boolean)(cos: CodedOutputStream) extends Writer(id, v)(cos) {
    override def write(): Unit = cos.writeBool(id, v)
  }

  case class StringWriter(id: Int, v: String)(cos: CodedOutputStream) extends Writer(id, v)(cos) {
    override def write(): Unit = cos.writeString(id, v)
  }

  case class EnumWriter(id: Int, v: EnumValueDescriptor)(cos: CodedOutputStream)
      extends Writer(id, v)(cos) { override def write(): Unit = cos.writeEnum(id, v.getNumber) }

  case class BytesWriter(id: Int, v: Array[Byte])(cos: CodedOutputStream)
      extends Writer(id, v)(cos) { override def write(): Unit = cos.writeByteArray(id, v) }

  case class MessageWriter(id: Int, v: Boolean)(cos: CodedOutputStream) extends Writer(id, v)(cos) {
    override def write(): Unit = cos.writeBool(id, v)
  }

  case class NullWriter()(cos: CodedOutputStream) extends Writer(0, ())(cos) {
    override def write(): Unit = ()
  }

  case class RepeatedWriter(v: List[PartialWriter])(cos: CodedOutputStream)
      extends Writer(0, v)(cos) {
    override def write(): Unit = v.foreach(pw => pw(cos).write())
  }
}

object ProtoBufGeneratorOps extends ProtoBufGeneratorOps

trait ProtoBufGeneratorOps {
  import ProtoBufWriters._

  private val cache: LoadingCache[Class[_], (Descriptors.Descriptor, Method)] = CacheBuilder
    .newBuilder()
    .build(new CacheLoader[Class[_], (Descriptors.Descriptor, Method)] {
      override def load(key: Class[_]): (Descriptor, Method) = {
        val desc = key.getMethod("getDescriptor").invoke(null).asInstanceOf[Descriptors.Descriptor]
        val parseFn = key.getMethod("parseFrom", classOf[Array[Byte]])
        (desc, parseFn)
      }
    })

  /** Generate a ProtoBuf Message */
  def protoBufOf[T <: AbstractMessage: ClassTag]: Gen[T] = {
    val (desc, parseFn) = cache.get(implicitly[ClassTag[T]].runtimeClass)
    val bytesGen = generate(desc)
    bytesGen.map(parseFn.invoke(null, _).asInstanceOf[T])
  }

  private def generate(desc: Descriptors.Descriptor): Gen[Array[Byte]] = {
    Gen
      .sequence[List[PartialWriter], PartialWriter](desc.getFields.asScala.map(genField))
      .map { l =>
        val baos = new ByteArrayOutputStream()
        val cos = CodedOutputStream.newInstance(baos)
        l.foreach(pw => pw(cos).write())
        cos.flush()
        baos.toByteArray
      }
  }

  private def genPositiveInt: Gen[Int] = Gen.chooseNum(0, Int.MaxValue)
  private def genPositiveLong: Gen[Long] = Gen.chooseNum(0L, Long.MaxValue)

  private def genField(field: FieldDescriptor): Gen[PartialWriter] = {
    val id = field.getNumber
    def genV: Gen[PartialWriter] = field.getType match {
      case Type.DOUBLE   => Arbitrary.arbDouble.arbitrary.map(v => DoubleWriter(id, v))
      case Type.FLOAT    => Arbitrary.arbFloat.arbitrary.map(v => FloatWriter(id, v))
      case Type.INT32    => Arbitrary.arbInt.arbitrary.map(v => Int32Writer(id, v))
      case Type.INT64    => Arbitrary.arbLong.arbitrary.map(v => Int64Writer(id, v))
      case Type.UINT32   => genPositiveInt.map(v => UInt32Writer(id, v))
      case Type.UINT64   => genPositiveLong.map(v => UInt64Writer(id, v))
      case Type.SINT32   => genPositiveInt.map(v => SInt32Writer(id, v))
      case Type.SINT64   => genPositiveLong.map(v => SInt64Writer(id, v))
      case Type.FIXED32  => genPositiveInt.map(v => Fixed32Writer(id, v))
      case Type.FIXED64  => genPositiveLong.map(v => Fixed64Writer(id, v))
      case Type.SFIXED32 => genPositiveInt.map(v => SFixed32Writer(id, v))
      case Type.SFIXED64 => genPositiveLong.map(v => SFixed64Writer(id, v))
      case Type.BOOL     => Arbitrary.arbBool.arbitrary.map(v => BoolWriter(id, v))
      case Type.STRING   => Arbitrary.arbString.arbitrary.map(v => StringWriter(id, v))

      case Type.ENUM =>
        Gen
          .oneOf(field.getEnumType.getValues.asScala)
          .map(e => EnumWriter(id, e))

      case Type.BYTES =>
        Gen
          .choose(1, 40)
          .flatMap(Gen.listOfN(_, Arbitrary.arbByte.arbitrary))
          .map(l => BytesWriter(id, l.toArray))

      case Type.MESSAGE =>
        generate(field.getMessageType)
          .map(b => BytesWriter(id, b))

      case t => throw new RuntimeException(s"Unsupported field type $t")
    }

    field.toProto.getLabel match {
      case Label.LABEL_OPTIONAL =>
        Arbitrary.arbBool.arbitrary.flatMap { e =>
          if (e) genV else NullWriter() _
        }
      case Label.LABEL_REQUIRED => genV
      case Label.LABEL_REPEATED => Gen.listOf(genV).map(l => RepeatedWriter(l))
    }
  }
}

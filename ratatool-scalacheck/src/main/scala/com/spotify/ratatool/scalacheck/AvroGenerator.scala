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

package com.spotify.ratatool.scalacheck

import java.io.ByteArrayOutputStream

import org.apache.avro._
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.avro.specific.{SpecificData, SpecificDatumReader, SpecificRecord}
import org.scalacheck.{Arbitrary, Gen}

import scala.reflect.ClassTag

/** Mainly type inference not to fall into `Any` */
class AvroValue(val value: Any) extends AnyVal

object AvroValue {
  def apply(x: Int): AvroValue = new AvroValue(x)
  def apply(x: Long): AvroValue = new AvroValue(x)
  def apply(x: Float): AvroValue = new AvroValue(x)
  def apply(x: Double): AvroValue = new AvroValue(x)
  def apply(x: Boolean): AvroValue = new AvroValue(x)
  def apply(x: Null): AvroValue = new AvroValue(x)
  def apply(x: String): AvroValue = new AvroValue(x)
  def apply(x: AnyRef): AvroValue = new AvroValue(x)
  def apply(x: GenericRecord): AvroValue = new AvroValue(x)
  def apply[A](x: Map[String, A]): AvroValue = new AvroValue(x)
  def apply(x: java.util.List[Any]): AvroValue = new AvroValue(x)
  def apply(x: Array[Byte]): AvroValue = new AvroValue(x)
}

object AvroGeneratorOps extends AvroGeneratorOps

trait AvroGeneratorOps {
  def specificRecordOf[A <: SpecificRecord: ClassTag]: Gen[A] = {
    val cls = implicitly[ClassTag[A]].runtimeClass.asInstanceOf[Class[A]]
    val schema = SpecificData.get().getSchema(cls)

    genericRecordOf(schema).map { generic =>
      val writer = new GenericDatumWriter[GenericRecord](schema)
      val out = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get().binaryEncoder(out, null)
      writer.write(generic, encoder)
      encoder.flush()

      val bytes = out.toByteArray

      val reader = new SpecificDatumReader(cls)
      val decoder = DecoderFactory.get().binaryDecoder(bytes, null)

      reader.read(null.asInstanceOf[A], decoder)
    }
  }

  def genericRecordOf(schema: Schema): Gen[GenericRecord] = {
    avroValueOf(schema).map { avroValue =>
      avroValue.value match {
        case x: GenericRecord => x
        case other => sys.error(s"Not a record: $schema")
      }
    }
  }

  // scalastyle:off cyclomatic.complexity
  // scalastyle:off method.length
  def avroValueOf(schema: Schema): Gen[AvroValue] = {
    import scala.collection.JavaConversions._

    schema.getType match {
      case Schema.Type.RECORD =>
        val record = new GenericData.Record(schema)

        val gens = schema.getFields.map { field =>
          avroValueOf(field.schema()).map { value =>
            record.put(field.pos(), value.value)
          }
        }

        Gen.sequence(gens).map(_ => AvroValue(record))

      case Schema.Type.UNION =>
        val gens = schema.getTypes.toList.map(avroValueOf)

        gens match {
          case Nil => Gen.fail
          case x :: Nil => x
          case x1 :: x2 :: xs => Gen.oneOf(x1, x2, xs: _*)
        }

      case Schema.Type.ARRAY =>
        val gen = avroValueOf(schema.getElementType)

        Gen.listOf(gen).map { xs =>
          val values = xs.map(_.value)
          val javaList = new java.util.ArrayList(asJavaCollection(values))

          AvroValue(javaList)
        }

      case Schema.Type.ENUM =>
        Gen.oneOf(schema.getEnumSymbols.toList)
          .map(GenericData.get().createEnum(_, schema))
          .map(AvroValue(_))

      case Schema.Type.MAP =>
        Gen.mapOf(
          for {
            k <- Gen.alphaStr
            v <- avroValueOf(schema.getElementType)
          } yield (k, v)
        ).map(AvroValue(_))

      case Schema.Type.FIXED =>
        Gen.listOfN(
          schema.getFixedSize,
          Arbitrary.arbByte.arbitrary
        ).map(x => AvroValue(x.toArray))

      case Schema.Type.STRING =>
        Gen.oneOf(
          Gen.oneOf[String](" ", "", "foo "),
          Arbitrary.arbString.arbitrary
        ).map(AvroValue(_))

      case Schema.Type.BYTES =>
        Gen.listOf(Arbitrary.arbByte.arbitrary).map(x => AvroValue(x.toArray))

      case Schema.Type.INT => Arbitrary.arbInt.arbitrary.map(AvroValue(_))
      case Schema.Type.LONG => Arbitrary.arbLong.arbitrary.map(AvroValue(_))
      case Schema.Type.FLOAT => Arbitrary.arbFloat.arbitrary.map(AvroValue(_))
      case Schema.Type.DOUBLE => Arbitrary.arbDouble.arbitrary.map(AvroValue(_))
      case Schema.Type.BOOLEAN => Arbitrary.arbBool.arbitrary.map(AvroValue(_))
      case Schema.Type.NULL => Gen.const(AvroValue(null))
    }
  }
}

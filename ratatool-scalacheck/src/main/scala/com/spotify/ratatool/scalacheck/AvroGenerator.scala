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

import java.nio.ByteBuffer
import java.util

import org.apache.avro._
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.specific.SpecificRecord
import org.apache.avro.util.Utf8
import org.apache.beam.sdk.coders.{AvroCoder, AvroGenericCoder}
import org.apache.beam.sdk.util.CoderUtils
import org.scalacheck.{Arbitrary, Gen}

import scala.reflect.ClassTag

/** Mainly type inference not to fall into `Any` */
class AvroValue(val value: Any) extends AnyVal

private object AvroValue {
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
    val specificCoder = AvroCoder.of(cls, true)
    val genericCoder = AvroGenericCoder.of(specificCoder.getSchema)

    genericRecordOf(specificCoder.getSchema).map { generic =>
      val bytes = CoderUtils.encodeToByteArray(genericCoder, generic)
      CoderUtils.decodeFromByteArray(specificCoder, bytes)
    }
  }

  def genericRecordOf(schema: Schema): Gen[GenericRecord] = {

    /**
     * Adding the Gen.const in front fixes behaviour with Gen.listOfN, which would previously return
     * the exact result for every item in the list. Also means that the behaviour of the generated
     * data matches that of other `Gen[T]`
     */
    Gen
      .const(0)
      .flatMap(_ =>
        avroValueOf(schema).map { avroValue =>
          avroValue.value match {
            case x: GenericRecord => x
            case other            => sys.error(s"Not a record: $schema")
          }
        }
      )
  }

  /** Aliases for API consistency across formats */
  def avroOf[A <: SpecificRecord: ClassTag]: Gen[A] = specificRecordOf[A]
  def avroOf(schema: Schema): Gen[GenericRecord] = genericRecordOf(schema)

  /**
   * Arbitrary [0-39] range and directly creating Utf-8 chosen to mimic [[RandomData]]. Also avoids
   * some ser/de issues with IndexOutOfBounds decoding with CoderUtils & Kryo
   */
  private def boundedLengthGen = Gen.chooseNum(0, 39)
  private def utf8Gen = boundedLengthGen
    .flatMap(n => Gen.listOfN(n, Arbitrary.arbChar.arbitrary))
    .map(l => new Utf8(l.mkString))

  // scalastyle:off cyclomatic.complexity
  // scalastyle:off method.length
  private def avroValueOf(schema: Schema): Gen[AvroValue] = {
    import scala.jdk.CollectionConverters._

    schema.getType match {
      case Schema.Type.RECORD =>
        val record = new GenericData.Record(schema)

        val gens = schema.getFields.asScala.map { field =>
          avroValueOf(field.schema()).map { value =>
            record.put(field.pos(), value.value)
          }
        }

        Gen.sequence(gens).map(_ => AvroValue(record))

      case Schema.Type.UNION =>
        val gens = schema.getTypes.asScala.toList.map(avroValueOf)

        gens match {
          case Nil            => Gen.fail
          case x :: Nil       => x
          case x1 :: x2 :: xs => Gen.oneOf(x1, x2, xs: _*)
        }

      case Schema.Type.ARRAY =>
        val gen = avroValueOf(schema.getElementType)

        Gen.listOf(gen).map { xs =>
          val al = new util.ArrayList[Any]()
          xs.map(_.value).foreach(al.add)
          AvroValue(al)
        }

      case Schema.Type.ENUM =>
        Gen
          .oneOf(schema.getEnumSymbols.asScala.toList)
          .map(GenericData.get().createEnum(_, schema))
          .map(AvroValue(_))

      /**
       * Directly creating a [[java.util.HashMap]] since JavaConverters `.asJava` method is lazy and
       * seems to cause issues with ser/de in Beam & Kryo resulting in flaky BigDiffy tests
       */
      case Schema.Type.MAP =>
        Gen
          .mapOf(
            for {
              k <- utf8Gen
              v <- avroValueOf(schema.getValueType)
            } yield (k, v.value)
          )
          .map { x =>
            val map: util.Map[Any, Any] = new util.HashMap[Any, Any]()
            x.foreach { case (k, v) => map.put(k, v) }
            AvroValue(map)
          }

      case Schema.Type.FIXED =>
        Gen
          .listOfN(
            schema.getFixedSize,
            Arbitrary.arbByte.arbitrary
          )
          .map(x => AvroValue(new GenericData.Fixed(schema, x.toArray)))

      case Schema.Type.STRING =>
        Gen
          .oneOf(
            Gen.oneOf[String](" ", "", "foo "),
            utf8Gen
          )
          .map(AvroValue(_))

      case Schema.Type.BYTES =>
        boundedLengthGen.flatMap(n =>
          Gen.listOfN(n, Arbitrary.arbByte.arbitrary).map { values =>
            val bb = ByteBuffer.wrap(values.toArray)
            AvroValue(bb)
          }
        )
      case Schema.Type.INT     => Arbitrary.arbInt.arbitrary.map(AvroValue(_))
      case Schema.Type.LONG    => Arbitrary.arbLong.arbitrary.map(AvroValue(_))
      case Schema.Type.FLOAT   => Arbitrary.arbFloat.arbitrary.map(AvroValue(_))
      case Schema.Type.DOUBLE  => Arbitrary.arbDouble.arbitrary.map(AvroValue(_))
      case Schema.Type.BOOLEAN => Arbitrary.arbBool.arbitrary.map(AvroValue(_))
      case Schema.Type.NULL    => Gen.const(AvroValue(null))
    }
  }
}

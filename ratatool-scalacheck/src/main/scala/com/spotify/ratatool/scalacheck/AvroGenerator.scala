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

import org.apache.avro._
import org.apache.avro.generic.{GenericData, GenericRecord, IndexedRecord}
import org.apache.avro.specific.{SpecificData, SpecificRecord}
import org.apache.avro.util.Utf8
import org.scalacheck.{Arbitrary, Gen}

import java.nio.ByteBuffer
import java.util
import scala.reflect.ClassTag
import scala.util.Try

object AvroGeneratorOps extends AvroGeneratorOps

trait AvroGeneratorOps {

  private lazy val avroRuntimeVersion =
    Option(classOf[Schema].getPackage.getImplementationVersion)

  // after avro 1.8, use SpecificData.getForClass
  private def dataForClass(cls: Class[_]): SpecificData = Try {
    val modelField = cls.getDeclaredField("MODEL$")
    modelField.setAccessible(true)
    val data = modelField.get(null).asInstanceOf[SpecificData]

    // avro 1.8 generated code does not add conversions to the data
    if (avroRuntimeVersion.exists(_.startsWith("1.8."))) {
      val conversionsField = cls.getDeclaredField("conversions")
      conversionsField.setAccessible(true)
      val conversions = conversionsField.get(null).asInstanceOf[Array[Conversion[_]]]
      conversions.filterNot(_ == null).foreach(data.addLogicalTypeConversion)
    }

    data
  }.recover { case _: NoSuchFieldException | _: IllegalAccessException =>
    // Return default instance
    SpecificData.get()
  }.get

  def specificRecordOf[A <: SpecificRecord: ClassTag]: Gen[A] = {
    val cls = implicitly[ClassTag[A]].runtimeClass.asInstanceOf[Class[A]]
    val data: SpecificData = dataForClass(cls)
    val schema = data.getSchema(cls)
    avroValueOf(schema)(data).asInstanceOf[Gen[A]]
  }
  def genericRecordOf(schema: Schema): Gen[GenericRecord] =
    avroValueOf(schema)(GenericData.get()).asInstanceOf[Gen[GenericRecord]]

  /** Aliases for API consistency across formats */
  def avroOf[A <: SpecificRecord: ClassTag]: Gen[A] = specificRecordOf[A]
  def avroOf(schema: Schema): Gen[GenericRecord] = genericRecordOf(schema)

  /**
   * Arbitrary [0-39] range and directly creating Utf-8 chosen to mimic [[RandomData]]. Also avoids
   * some ser/de issues with IndexOutOfBounds decoding with CoderUtils & Kryo
   */
  private def boundedLengthGen: Gen[Int] = Gen.chooseNum(0, 39)

  private val genString: Gen[String] = Gen.oneOf(
    Gen.oneOf(" ", "", "foo"),
    Arbitrary.arbString.arbitrary
  )

  private val genUri: Gen[java.net.URI] = for {
    scheme <- Gen.option(Gen.oneOf("http", "https", "ftp"))
    userInfo <- Gen.option(Gen.alphaNumStr)
    host <- Gen.oneOf("example.com", "example.org", "example.net")
    port <- Gen.option(Gen.choose(1, 65535))
    segments <- Gen.option(Gen.nonEmptyListOf(Gen.alphaNumStr))
    params <- Gen.option(Gen.nonEmptyListOf(Gen.alphaNumStr))
    fragment <- Gen.option(Gen.alphaNumStr)
  } yield {
    val authority = (userInfo.map(_ + "@") ++ Some(host) ++ port.map(":" + _)).mkString
    val path = segments.map(_.mkString("/", "/", ""))
    val query = params.map(_.mkString("&"))
    new java.net.URI(scheme.orNull, authority, path.orNull, query.orNull, fragment.orNull)
  }

  private val genUrl: Gen[java.net.URL] = for {
    scheme <- Gen.oneOf("http", "https", "ftp")
    userInfo <- Gen.option(Gen.alphaNumStr)
    host <- Gen.oneOf("example.com", "example.org", "example.net")
    port <- Gen.option(Gen.choose(1, 65535))
    segments <- Gen.nonEmptyListOf(Gen.alphaNumStr)
    params <- Gen.option(Gen.nonEmptyListOf(Gen.alphaNumStr))
    fragment <- Gen.option(Gen.alphaNumStr)
  } yield {
    val authority = (userInfo.map(_ + "@") ++ Some(host) ++ port.map(":" + _)).mkString
    val path = segments.mkString("/", "/", "")
    val query = params.map(_.mkString("&"))
    new java.net.URI(scheme, authority, path, query.orNull, fragment.orNull).toURL
  }

  private val genFile: Gen[java.io.File] = for {
    prefix <- Gen.oneOf("", "/", "./", "../")
    segments <- Gen.nonEmptyListOf(Gen.alphaNumStr)
    extension <- Gen.option(Gen.stringOfN(3, Gen.alphaNumChar))
  } yield {
    val suffix = extension.map("." + _).getOrElse("")
    new java.io.File(segments.mkString(prefix, "/", suffix))
  }

  private def genStringable(clazz: Class[_]): Gen[Any] = {
    if (clazz == classOf[java.math.BigDecimal]) {
      Gen.choose(BigDecimal(Int.MinValue), BigDecimal(Int.MaxValue)).map(_.bigDecimal)
    } else if (clazz == classOf[java.math.BigInteger]) {
      Gen.choose(BigInt(Int.MinValue), BigInt(Int.MaxValue)).map(_.bigInteger)
    } else if (clazz == classOf[java.net.URI]) {
      genUri
    } else if (clazz == classOf[java.net.URL]) {
      genUrl
    } else if (clazz == classOf[java.io.File]) {
      genFile
    } else {
      val ctor = clazz.getDeclaredConstructor(classOf[String])
      ctor.setAccessible(true)
      genString.map(ctor.newInstance(_))
    }
  }

  private def genAvroString(tpe: Schema): Gen[CharSequence] = {
    Option(tpe.getProp(GenericData.STRING_PROP)) match {
      case Some("String") => genString
      case _              => genString.map(new Utf8(_))
    }
  }

  private def avroValueOf(schema: Schema)(implicit data: GenericData): Gen[Any] = {
    import scala.jdk.CollectionConverters._

    val conversion = for {
      logicalType <- Option(schema.getLogicalType)
      conversion <- Option(data.getConversionFor(logicalType))
    } yield conversion

    schema.getType match {
      case Schema.Type.RECORD =>
        val recordData = data match {
          case specificData: SpecificData => dataForClass(specificData.getClass(schema))
          case _                          => data
        }

        val record = for {
          fields <- Gen.sequence[List[(Int, Any)], (Int, Any)](schema.getFields.asScala.map { f =>
            avroValueOf(f.schema())(recordData).map(v => f.pos() -> v)
          })
        } yield fields.foldLeft(recordData.newRecord(null, schema).asInstanceOf[IndexedRecord]) {
          case (r, (idx, v)) =>
            r.put(idx, v)
            r
        }
        conversion match {
          case Some(c) => record.map(r => c.fromRecord(r, schema, schema.getLogicalType))
          case None    => record
        }

      case Schema.Type.UNION =>
        val types = schema.getTypes.asScala
        for {
          i <- Gen.choose(0, types.size - 1)
          t <- avroValueOf(types(i))
        } yield t

      case Schema.Type.ARRAY =>
        import org.scalacheck.util.Buildable._
        implicit val tt: util.ArrayList[Any] => Traversable[Any] = _.asScala
        val array = Gen.containerOf[util.ArrayList, Any](avroValueOf(schema.getElementType))
        conversion match {
          case Some(c) => array.map(a => c.fromArray(a, schema, schema.getLogicalType))
          case None    => array
        }

      case Schema.Type.ENUM =>
        for {
          symbol <- Gen.oneOf(schema.getEnumSymbols.asScala)
        } yield conversion match {
          case Some(c) =>
            val enumSymbol = new GenericData.EnumSymbol(schema, symbol)
            c.fromEnumSymbol(enumSymbol, schema, schema.getLogicalType)
          case None =>
            data.createEnum(symbol, schema)
        }

      case Schema.Type.MAP =>
        import HashMapBuildable._
        Option(schema.getProp(SpecificData.KEY_CLASS_PROP)) match {
          case Some(cls) =>
            Gen.buildableOf[util.HashMap[Any, Any], (Any, Any)](
              (genStringable(Class.forName(cls)), avroValueOf(schema.getValueType)).tupled
            )
          case None =>
            val map = Gen.buildableOf[util.HashMap[CharSequence, Any], (CharSequence, Any)](
              (genAvroString(schema), avroValueOf(schema.getValueType)).tupled
            )
            conversion match {
              case Some(c) => map.map(m => c.fromMap(m, schema, schema.getLogicalType))
              case None    => map
            }
        }

      case Schema.Type.FIXED =>
        for {
          bytes <- Gen.listOfN(schema.getFixedSize, Arbitrary.arbByte.arbitrary).map(_.toArray)
        } yield conversion match {
          case Some(c) =>
            val fixed = new GenericData.Fixed(schema, bytes)
            c.fromFixed(fixed, schema, schema.getLogicalType)
          case None => data.createFixed(null, bytes, schema)
        }

      case Schema.Type.STRING =>
        Option(schema.getProp(SpecificData.CLASS_PROP)) match {
          case Some(cls) => genStringable(Class.forName(cls))
          case None =>
            val str = genAvroString(schema)
            conversion match {
              case Some(c) => str.map(cs => c.fromCharSequence(cs, schema, schema.getLogicalType))
              case None    => str
            }
        }

      case Schema.Type.BYTES =>
        val bytes = for {
          n <- boundedLengthGen
          bs <- Gen.listOfN(n, Arbitrary.arbByte.arbitrary)
        } yield ByteBuffer.wrap(bs.toArray)
        conversion match {
          case Some(c) =>
            val bs = schema.getLogicalType match {
              case dt: LogicalTypes.Decimal =>
                // we can't convert random bytes
                val max = BigInt(10).pow(dt.getPrecision) - 1
                Gen.choose(-max, max).map(bs => ByteBuffer.wrap(bs.toByteArray))
              case _ =>
                bytes
            }
            bs.map(b => c.asInstanceOf[Conversion[Any]].fromBytes(b, schema, schema.getLogicalType))
          case None => bytes
        }

      case Schema.Type.INT =>
        val int = Arbitrary.arbInt.arbitrary
        conversion match {
          case Some(c) => int.map(i => c.fromInt(i, schema, schema.getLogicalType))
          case None    => int
        }
      case Schema.Type.LONG =>
        val long = Arbitrary.arbLong.arbitrary
        conversion match {
          case Some(c) => long.map(l => c.fromLong(l, schema, schema.getLogicalType))
          case None    => long
        }

      case Schema.Type.FLOAT =>
        val float = Arbitrary.arbFloat.arbitrary
        conversion match {
          case Some(c) => float.map(f => c.fromFloat(f, schema, schema.getLogicalType))
          case None    => float
        }

      case Schema.Type.DOUBLE =>
        val double = Arbitrary.arbDouble.arbitrary
        conversion match {
          case Some(c) => double.map(d => c.fromDouble(d, schema, schema.getLogicalType))
          case None    => double
        }

      case Schema.Type.BOOLEAN =>
        val bool = Arbitrary.arbBool.arbitrary
        conversion match {
          case Some(c) => bool.map(b => c.fromBoolean(b, schema, schema.getLogicalType))
          case None    => bool
        }

      case Schema.Type.NULL => Gen.const(null)
    }
  }
}

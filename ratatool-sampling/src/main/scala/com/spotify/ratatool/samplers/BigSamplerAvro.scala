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

import java.nio.ByteBuffer
import java.util.{List => JList}

import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.Hasher
import com.spotify.ratatool.io.{AvroIO, FileStorage}
import com.spotify.ratatool.samplers.util._
import com.spotify.scio.ScioContext
import com.spotify.scio.avro._
import com.spotify.scio.io.{ClosedTap, MaterializeTap}
import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.generic._
import org.apache.avro.specific.SpecificData
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import com.spotify.scio.avro._
import com.spotify.scio.coders.Coder

private[samplers] object BigSamplerAvro {
  private val log = LoggerFactory.getLogger(BigSamplerAvro.getClass)

  private def resolveUnion(schema: Schema, value: Object): Schema = {
    if (schema.getType == Type.UNION) {
      schema.getTypes.get(SpecificData.get().resolveUnion(schema, value))
    } else {
      schema
    }
  }

  private[samplers] def getField(
    r: GenericRecord,
    fieldPath: List[String],
    schema: Schema
  ): (String, Schema, AnyRef) = {
    // Resolve the record in case it is a union
    val recordSchema = resolveUnion(schema, r)

    // Get the field using the resolved schema
    val field = Some(recordSchema.getField(fieldPath.head)).getOrElse {
      throw new NoSuchElementException(s"Can't find field ${fieldPath.head} in avro schema $schema")
    }

    // Get the value of the field
    val fieldValue = r.get(fieldPath.head)

    // Get the resolved field schema for unions
    val fieldSchema = resolveUnion(field.schema, fieldValue)

    (field.name, fieldSchema, fieldValue)
  }

  /**
   * Builds a key function per record Sets do not have deterministic ordering so we return a sorted
   * list
   */
  private[samplers] def buildKey(schema: => Schema, distributionFields: Seq[String])(
    gr: GenericRecord
  ): List[String] = {
    distributionFields
      .map { f =>
        val fieldValue = getAvroField(gr, f.split(BigSampler.fieldSep).toList, schema)
        // Avro caches toString in the Utf8 class which results in an IllegalMutationException
        // later on if we don't materialize the string once before. Ignoring this prevents us
        // from printing the key in e.g. SamplerSCollectionFunctions.logDistributionDiffs.

        // It's not possible to derive compile-time coders for Any or AnyRef/Object, so we return a
        // Set[String] with the toString values for each field to use as our key.
        // Of course, if it's null we can't call toString on it, so we wrap.
        Option(fieldValue).map(_.toString).getOrElse("null")
      }
      .toSet
      .toList
      .sorted
  }

  @tailrec
  private[samplers] def getAvroField(
    r: GenericRecord,
    fieldPath: List[String],
    schema: Schema
  ): Any = {

    val (fieldName, fieldSchema, fieldValue) = getField(r, fieldPath, schema)

    if (fieldValue == null) {
      log.debug(
        s"Field `${fieldName}` of type ${fieldSchema.getType} is null, will not look" +
          " for nested values"
      )
      null
    } else {
      fieldSchema.getType match {
        case Type.RECORD if fieldPath.tail.nonEmpty =>
          getAvroField(fieldValue.asInstanceOf[GenericRecord], fieldPath.tail, fieldSchema)
        case Type.ARRAY | Type.MAP =>
          throw new UnsupportedOperationException(
            s"Type `${fieldSchema.getType}` of `${fieldName}` is not supported as stratification " +
              s"key!"
          )
        case _ => fieldValue
      }
    }
  }

  private[samplers] def hashAvroField(
    schema: Schema
  )(r: GenericRecord, fieldStr: String, hasher: Hasher): Hasher =
    hashAvroField(schema, r, fieldStr.split(BigSampler.fieldSep).toList, hasher)

  private[samplers] def hashAvroField(
    schema: Schema,
    r: GenericRecord,
    fieldPath: List[String],
    hasher: Hasher
  ): Hasher = {
    val (fieldName, fieldSchema, fieldValue) = getField(r, fieldPath, schema)
    if (fieldValue == null) {
      log.debug(
        s"Field `${fieldName}` of type ${fieldSchema.getType} is null - won't account for hash"
      )
      hasher
    } else {
      val vs = if (fieldSchema.getType == Type.ARRAY) {
        val elementType = fieldSchema.getElementType
        fieldValue.asInstanceOf[JList[AnyRef]].asScala.map(v => (v, resolveUnion(elementType, v)))
      } else {
        Seq((fieldValue, fieldSchema))
      }

      vs.foldLeft(hasher) { case (h, (v, s)) =>
        s.getType match {
          case Type.RECORD =>
            hashAvroField(s, v.asInstanceOf[GenericRecord], fieldPath.tail, hasher)
          case _ => hashPrimitive(fieldName, s, v, h)
        }
      }
    }
  }

  private def hashPrimitive(
    fieldName: String,
    fieldSchema: Schema,
    fieldValue: AnyRef,
    hasher: Hasher
  ): Hasher = {
    fieldSchema.getType match {
      case Type.ENUM => hashEnum(fieldName, fieldSchema, fieldValue, hasher)
      case Type.STRING =>
        hasher.putString(fieldValue.asInstanceOf[CharSequence], BigSampler.utf8Charset)
      case Type.BYTES => hashBytes(fieldName, fieldSchema, fieldValue, hasher)
      // to keep it consistent with BigQuery INT - convert int to long
      case Type.INT     => hasher.putLong(fieldValue.asInstanceOf[Int].toLong)
      case Type.LONG    => hasher.putLong(fieldValue.asInstanceOf[Long])
      case Type.FLOAT   => hasher.putFloat(fieldValue.asInstanceOf[Float])
      case Type.DOUBLE  => hasher.putDouble(fieldValue.asInstanceOf[Double])
      case Type.BOOLEAN => hasher.putBoolean(fieldValue.asInstanceOf[Boolean])
      case Type.FIXED   => hashBytes(fieldName, fieldSchema, fieldValue, hasher)
      case Type.NULL    => hasher // Ignore nulls
      case t =>
        throw new UnsupportedOperationException(
          s"Type `${fieldSchema.getType}` of `${fieldName}` is not supported as sampling key!"
        )
    }
  }

  private def hashEnum(
    fieldName: String,
    fieldSchema: Schema,
    fieldValue: AnyRef,
    hasher: Hasher
  ): Hasher = {
    // Enum has two possible types depending on if v came from a specific or generic record
    fieldValue match {
      case sv: Enum[_]                => hasher.putString(sv.name, BigSampler.utf8Charset)
      case gv: GenericData.EnumSymbol => hasher.putString(gv.toString, BigSampler.utf8Charset)
      case _ =>
        throw new UnsupportedOperationException(
          s"Internal type of `${fieldName}` not consistent with `${fieldSchema.getType}`!"
        )
    }
  }

  private def hashBytes(
    fieldName: String,
    fieldSchema: Schema,
    fieldValue: AnyRef,
    hasher: Hasher
  ): Hasher = {
    // Types depend on whether v came from a specific or generic record
    fieldValue match {
      case sv: Array[Byte]  => hasher.putBytes(sv)
      case gv: ByteBuffer   => hasher.putBytes(gv.array())
      case fv: GenericFixed => hasher.putBytes(fv.bytes())
      case _ =>
        throw new UnsupportedOperationException(
          s"Internal type of `${fieldName}` not consistent with `${fieldSchema.getType}`!"
        )
    }
  }

  private[samplers] def sample(
    sc: ScioContext,
    input: String,
    output: String,
    fields: Seq[String],
    fraction: Double,
    seed: Option[Int],
    hashAlgorithm: HashAlgorithm,
    distribution: Option[SampleDistribution],
    distributionFields: Seq[String],
    precision: Precision,
    maxKeySize: Int,
    byteEncoding: ByteEncoding = RawEncoding
  ): ClosedTap[GenericRecord] = {
    val schema = AvroIO.getAvroSchemaFromFile(input)
    val outputParts = if (output.endsWith("/")) output + "part*" else output + "/part*"
    implicit val coder: Coder[GenericRecord] = avroGenericRecordCoder(schema)

    if (FileStorage(outputParts).isDone) {
      log.info(s"Reuse previous sample at $outputParts")
      ClosedTap(MaterializeTap[GenericRecord](outputParts, sc))
    } else {
      log.info(s"Will sample from: $input, output will be $output")
      val coll = sc.avroFile(input, schema)

      val sampledCollection = sampleAvro(
        coll,
        fraction,
        schema,
        fields,
        seed,
        hashAlgorithm,
        distribution,
        distributionFields,
        precision,
        maxKeySize,
        byteEncoding
      )

      val r = sampledCollection.saveAsAvroFile(output, schema = schema)
      sc.run().waitUntilDone()
      r
    }
  }
}

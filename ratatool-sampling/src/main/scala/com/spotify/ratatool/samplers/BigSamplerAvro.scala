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

import java.io.IOException
import java.util.{List => JList}

import com.google.common.hash.Hasher
import com.spotify.ratatool.io.{AvroIO, FileStorage}
import com.spotify.ratatool.samplers.util.{Precision, SampleDistribution}
import com.spotify.scio.ScioContext
import com.spotify.scio.io.{Tap, Taps}
import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericRecord
import org.slf4j.LoggerFactory


import scala.collection.JavaConverters._
import scala.annotation.tailrec
import scala.concurrent.Future

private[samplers] object BigSamplerAvro {
  private val log = LoggerFactory.getLogger(BigSamplerAvro.getClass)

  // scalastyle:off cyclomatic.complexity
  @tailrec
  private[samplers] def hashAvroField(schema: Schema)(r: GenericRecord,
                                                      fieldStr: String,
                                                      hasher: Hasher): Hasher = {
    val recordSchema = getRecordSchema(schema)
    val subfields = fieldStr.split(BigSampler.fieldSep)
    val field = recordSchema.getFields.asScala.find(_.name == subfields.head).getOrElse {
      throw new NoSuchElementException(s"Can't find field $fieldStr in avro schema $schema")
    }
    val v = r.get(subfields.head)
    if (v == null) {
      log.debug(s"Field `${field.name}` of type ${field.schema.getType} is null - won't account" +
        s" for hash")
      hasher
    } else {
      field.schema.getType match {
        case Type.RECORD =>
          hashAvroField(field.schema)(
            v.asInstanceOf[GenericRecord],
            subfields.tail.mkString(BigSampler.fieldSep.toString),
            hasher)
        case Type.ENUM => hasher.putString(v.asInstanceOf[Enum[_]].name, BigSampler.utf8Charset)
        case Type.STRING => hasher.putString(v.asInstanceOf[CharSequence], BigSampler.utf8Charset)
        case Type.BYTES => hasher.putBytes(v.asInstanceOf[Array[Byte]])
        // to keep it consistent with BigQuery INT - convert int to long
        case Type.INT => hasher.putLong(v.asInstanceOf[Int].toLong)
        case Type.LONG => hasher.putLong(v.asInstanceOf[Long])
        case Type.FLOAT => hasher.putFloat(v.asInstanceOf[Float])
        case Type.DOUBLE => hasher.putDouble(v.asInstanceOf[Double])
        case Type.BOOLEAN => hasher.putBoolean(v.asInstanceOf[Boolean])
        case Type.UNION => hashAvroUnionField(field, v, hasher)
        case Type.ARRAY => hashAvroArrayField(field, v, hasher)
        //  Type.MAP | Type.FIXED =>
        case t => throw new UnsupportedOperationException(
          s"Type `${field.schema.getType}` of `${field.name}` is not supported as sampling key!")
      }
    }
  }
  // scalastyle:on cyclomatic.complexity

  // scalastyle:off cyclomatic.complexity
  private def hashAvroArrayField(field: Schema.Field, v: AnyRef, hasher: Hasher): Hasher = {
    field.schema.getElementType.getType match {
      case Type.ENUM => v.asInstanceOf[JList[Enum[_]]].asScala.foldLeft(hasher)((hasher, e) =>
        hasher.putString(e.name, BigSampler.utf8Charset))
      case Type.STRING => v.asInstanceOf[JList[CharSequence]].asScala.foldLeft(hasher)(
        (hasher, e) => hasher.putString(e, BigSampler.utf8Charset))
      case Type.BYTES => v.asInstanceOf[JList[Array[Byte]]].asScala.foldLeft(hasher)((hasher, e) =>
        hasher.putBytes(e))
      case Type.INT => v.asInstanceOf[JList[Int]].asScala.foldLeft(hasher)((hasher, e) =>
        // to keep it consistent with BigQuery INT - convert int to long
        hasher.putLong(e.toLong))
      case Type.LONG => v.asInstanceOf[JList[Long]].asScala.foldLeft(hasher)((hasher, e) =>
        hasher.putLong(e))
      case Type.FLOAT => v.asInstanceOf[JList[Float]].asScala.foldLeft(hasher)((hasher, e) =>
        hasher.putFloat(e))
      case Type.DOUBLE => v.asInstanceOf[JList[Double]].asScala.foldLeft(hasher)((hasher, e) =>
        hasher.putDouble(e))
      case Type.BOOLEAN => v.asInstanceOf[JList[Boolean]].asScala.foldLeft(hasher)((hasher, e) =>
        hasher.putBoolean(e))
      case Type.NULL =>
        // ignore nulls
        hasher
      case _ => throw new UnsupportedOperationException(
        s"Type ${field.schema.getElementType.getType} is not supported as hash for array ")
    }
  }

  // scalastyle:off cyclomatic.complexity
  private def hashAvroUnionField(field: Schema.Field, v: AnyRef, hasher: Hasher): Hasher = {
    val types = field.schema.getTypes.asScala
    types.foldLeft(hasher)( (hasher, p) =>
      p.getType match {
        case Type.ENUM if v.isInstanceOf[Enum[_]] =>
          hasher.putString(v.asInstanceOf[Enum[_]].name, BigSampler.utf8Charset)
        case Type.STRING if v.isInstanceOf[CharSequence] =>
          hasher.putString(v.asInstanceOf[CharSequence], BigSampler.utf8Charset)
        case Type.BYTES if v.isInstanceOf[Array[Byte]] =>
          hasher.putBytes(v.asInstanceOf[Array[Byte]])
        // to keep it consistent with BigQuery INT - convert int to long
        case Type.INT if v.isInstanceOf[Int] => hasher.putLong(v.asInstanceOf[Int].toLong)
        case Type.LONG if v.isInstanceOf[Long] => hasher.putLong(v.asInstanceOf[Long])
        case Type.FLOAT if v.isInstanceOf[Float] => hasher.putFloat(v.asInstanceOf[Float])
        case Type.DOUBLE if v.isInstanceOf[Double] => hasher.putDouble(v.asInstanceOf[Double])
        case Type.BOOLEAN if v.isInstanceOf[Boolean] =>
          hasher.putBoolean(v.asInstanceOf[Boolean])
        case Type.NULL =>
          // ignore nulls
          hasher
        case _ => throw new UnsupportedOperationException(
          s"Value `$v` of union ${field.name} has unsupported type `${p.getType}`")
      }
    )
  }
  // scalastyle:on cyclomatic.complexity

  private def getRecordSchema(schema: Schema): Schema = {
    schema.getType match {
      case Type.UNION => schema.getTypes.asScala.head
      case Type.RECORD => schema
      case _ => throw new IOException(s"Can't recognise schema `$schema` as record")
    }
  }

  @tailrec
  private def fieldInAvroSchema(schema: Schema, fieldStr: String): Boolean = {
    val recordSchema = getRecordSchema(schema)
    val subfields = fieldStr.split(BigSampler.fieldSep)
    val fieldOpt = recordSchema.getFields.asScala.find(_.name == subfields.head)
    if (fieldOpt.isEmpty) {
      false
    } else {
      val field = fieldOpt.get
      field.schema.getType match {
        case Type.RECORD =>
          fieldInAvroSchema(field.schema, subfields.tail.mkString(BigSampler.fieldSep.toString))
        case Type.ENUM | Type.STRING | Type.BYTES | Type.INT | Type.LONG| Type.FLOAT | Type.DOUBLE
             | Type.BOOLEAN => true
        case  t => throw new UnsupportedOperationException(
          s"Type `${field.schema.getType}` of `${field.name}` is not supported as sampling key!")
      }
    }
  }

  // scalastyle:off cyclomatic.complexity
  // TODO: Potentially reduce this and hashAvroField to a single function
  @tailrec
  private[samplers] def getAvroField(r: GenericRecord,
                                     fieldStr: String,
                                     schema: Schema): Any = {
    val recordSchema = getRecordSchema(schema)
    val subfields = fieldStr.split(BigSampler.fieldSep)
    val field = recordSchema.getFields.asScala.find(_.name == subfields.head).getOrElse {
      throw new NoSuchElementException(s"Can't find field $fieldStr in avro schema $schema")
    }
    val v = r.get(subfields.head)
    if (v == null) {
      log.debug(s"Field `${field.name}` of type ${field.schema.getType} is null, will not look" +
        " for nested values")
      null
    } else {
      field.schema.getType match {
        case Type.RECORD if fieldStr.nonEmpty =>
          getAvroField(
            v.asInstanceOf[GenericRecord],
            subfields.tail.mkString(BigSampler.fieldSep.toString),
            field.schema)
        case Type.ENUM => v.asInstanceOf[Enum[_]]
        case Type.STRING => v.asInstanceOf[CharSequence]
        case Type.BYTES => v.asInstanceOf[Array[Byte]]
        case Type.INT => v.asInstanceOf[Int].toLong
        case Type.LONG => v.asInstanceOf[Long]
        case Type.FLOAT => v.asInstanceOf[Float]
        case Type.DOUBLE => v.asInstanceOf[Double]
        case Type.BOOLEAN => v.asInstanceOf[Boolean]
        //TODO: Support Union type
        case _ => throw new Exception(s"Current type ${field.schema.getType} is unsupported " +
          s"or field $fieldStr does not refer to a single field.")
      }
    }
  }
  // scalastyle:on cyclomatic.complexity

  private[samplers] def buildKey(schema: => Schema,
                                 distributionFields: Seq[String])(gr: GenericRecord)
  : Set[String] = {
    distributionFields.map(f => getAvroField(gr, f, schema).toString).toSet
  }

  //scalastyle:off method.length cyclomatic.complexity parameter.number
  private[samplers] def sample(sc: ScioContext,
                               input: String,
                               output: String,
                               fields: Seq[String],
                               fraction: Double,
                               seed: Option[Int],
                               distribution: Option[SampleDistribution],
                               distributionFields: Seq[String],
                               precision: Precision,
                               maxKeySize: Int)
  : Future[Tap[GenericRecord]] = {
    val schema = AvroIO.getAvroSchemaFromFile(input)
    val outputParts = if (output.endsWith("/")) output + "part*" else output + "/part*"
    if (FileStorage(outputParts).isDone) {
      log.info(s"Reuse previous sample at $outputParts")
      Taps().avroFile(outputParts, schema = schema)
    } else {
      log.info(s"Will sample from: $input, output will be $output")
      val schemaSer = schema.toString(false)
      @transient lazy val schemaSerDe = new Schema.Parser().parse(schemaSer)

      val coll = sc.avroFile[GenericRecord](input, schema)

      val sampledCollection = sampleAvro(coll, fraction, schema, fields, seed, distribution,
        distributionFields, precision, maxKeySize)


      val r = sampledCollection.saveAsAvroFile(output, schema = schema)
      sc.close().waitUntilDone()
      r
    }
  }
  //scalastyle:on method.length cyclomatic.complexity parameter.number
}

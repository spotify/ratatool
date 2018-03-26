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
package com.spotify.ratatool.samplers

import java.io.IOException
import java.net.URI
import java.nio.charset.Charset
import java.util.{List => JList}

import com.google.api.services.bigquery.model.{TableFieldSchema, TableReference, TableSchema}
import com.google.common.hash.{HashCode, HashFunction, Hasher, Hashing}
import com.google.common.io.BaseEncoding
import com.spotify.ratatool.avro.specific.TestRecord
import com.spotify.ratatool.io.{AvroIO, FileStorage}
import com.spotify.ratatool.serde.JsonSerDe
import com.spotify.scio.bigquery.TableRow
import com.spotify.scio.io.{Tap, Taps}
import com.spotify.scio.{ContextAndArgs, ScioContext}
import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.io.gcp.bigquery.{BigQueryHelpers,
                                            BigQueryIO,
                                            BigQueryOptions,
                                            BigQueryServicesImpl}
import org.apache.beam.sdk.options.PipelineOptions
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.language.existentials
import scala.util.Try

object BigSampler {
  private val log = LoggerFactory.getLogger(BigSampler.getClass)
  private[samplers] val utf8Charset = Charset.forName("UTF-8")
  private[samplers] val fieldSep = '.'

  private[samplers] def kissHashFun(seed: Option[Int] = None): HashFunction = {
    seed match {
      case Some(s) => Hashing.murmur3_128(s)
      case None => Hashing.murmur3_128(System.currentTimeMillis.toInt)
    }
  }

  /**
   * Internal element dicing method.
   *
   * @param samplePct (0.0, 100.0]
   */
  private[samplers] def diceElement[T](e: T, hash: HashCode, samplePct: Double): Option[T] = {
    //TODO: for now leave it up to jit/compiler to optimize
    if (math.abs(hash.asLong) % 100.0 < samplePct) {
      Some(e)
    } else {
      None
    }
  }

  private def parseAsBigQueryTable(tblRef: String): Option[TableReference] = {
    Try(BigQueryHelpers.parseTableSpec(tblRef)).toOption
  }

  private def parseAsURI(uri: String): Option[URI] = {
    Try(new URI(uri)).toOption
  }

  private[samplers] def hashTableRow(r: TableRow,
                                     f: String,
                                     tblSchemaFields: Seq[TableFieldSchema],
                                     hasher: Hasher): Hasher =
    BigSamplerBigQuery.hashTableRow(r, f, tblSchemaFields, hasher)

  private[samplers] def hashAvroField(r: TestRecord,
                                      f: String,
                                      avroSchema: Schema,
                                      hasher: Hasher): Hasher =
    BigSamplerAvro.hashAvroField(r, f, avroSchema, hasher)

  def singleInput(argv: Array[String]): Future[Tap[_]] = {
    val (sc, args) = ContextAndArgs(argv)
    val (opts, _) = ScioContext.parseArguments[PipelineOptions](argv)
    val samplePct = args("sample").toFloat

    require(samplePct > 0.0F && samplePct <= 1.0F,
      "Sample percentage should be between (0.0, 1.0]")

    val input = args("input")
    val output = args("output")
    val fields = args.list("fields")
    val seed = args.optional("seed")

    if (fields.isEmpty) {
      log.warn("No fields to hash on specified, won't guarantee cohorts between datasets.")
    }

    if (seed.isEmpty) {
      log.warn("No seed specified, won't guarantee cohorts between datasets.")
    }

    if (parseAsBigQueryTable(input).isDefined) {
      require(parseAsBigQueryTable(output).isDefined,
        s"Input is a BigQuery table `$input`, output should be a BigQuery table too," +
          s"but instead it's `$output`.")
      val inputTbl = parseAsBigQueryTable(input).get
      val outputTbl = parseAsBigQueryTable(output).get
      BigSamplerBigQuery.sampleBigQueryTable(
        sc,
        inputTbl,
        outputTbl,
        fields,
        samplePct,
        seed.map(_.toInt))
    } else if (parseAsURI(input).isDefined) {
      // right now only support for avro
      require(parseAsURI(output).isDefined,
        s"Input is a URI: `$input`, output should be a URI too, but instead it's `$output`.")
      // Prompts FileSystems to load service classes, otherwise fetching schema from non-local fails
      FileSystems.setDefaultPipelineOptions(opts)
      BigSamplerAvro.sampleAvro(sc, input, output, fields, samplePct, seed.map(_.toInt))
    } else {
      throw new UnsupportedOperationException(s"Input `$input not supported.")
    }
  }

  def main(argv: Array[String]): Unit = {
    this.singleInput(argv)
  }
}

private[samplers] object BigSamplerAvro {
  private val log = LoggerFactory.getLogger(BigSamplerAvro.getClass)

  // scalastyle:off cyclomatic.complexity
  @tailrec
  private[samplers] def hashAvroField(r: GenericRecord,
                                      fieldStr: String,
                                      schema: Schema,
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
          hashAvroField(
            v.asInstanceOf[GenericRecord],
            subfields.tail.mkString(BigSampler.fieldSep.toString),
            field.schema,
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

  private[samplers] def sampleAvro(sc: ScioContext,
                         input: String,
                         output: String,
                         fields: Seq[String],
                         samplePct: Float,
                         seed: Option[Int]): Future[Tap[GenericRecord]] = {
    val schema = AvroIO.getAvroSchemaFromFile(input)
    val outputParts =
      if (output.endsWith("/")) output + "part*" else output + "/part*"
    if (FileStorage(outputParts).isDone) {
      log.info(s"Reuse previous sample at $outputParts")
      Taps().avroFile(outputParts, schema = schema)
    } else {
      log.info(s"Will sample from: $input, output will be $output")
      if (fields.isEmpty) {
        val r = sc.avroFile[GenericRecord](input, schema)
          .sample(false, samplePct)
          .saveAsAvroFile(output, schema=schema)
        sc.close().waitUntilDone()
        r
      } else {
        val fieldsMissing = fields.filterNot(f => fieldInAvroSchema(schema, f))
        if (fieldsMissing.nonEmpty) {
          throw new NoSuchElementException(
            s"""Could not locate field(s) ${fieldsMissing.mkString(",")} """ +
              s"""in $input with schema $schema""")
        }
        val schemaSer = schema.toString(false)
        @transient lazy val schemaSerDe = new Schema.Parser().parse(schemaSer)
        val samplePct100 = samplePct * 100.0
        val r = sc.avroFile[GenericRecord](input, schema)
          .flatMap { e =>
            val hasher = BigSampler.kissHashFun(seed).newHasher(fields.size)
            val hash = fields.foldLeft(hasher)((h, f) => hashAvroField(e, f, schemaSerDe, h)).hash
            BigSampler.diceElement(e, hash, samplePct100)
          }
          .saveAsAvroFile(output, schema = schema)
        sc.close().waitUntilDone()
        r
      }
    }
  }
}

private[samplers] object BigSamplerBigQuery {

  private val log = LoggerFactory.getLogger(BigSamplerBigQuery.getClass)

  // scalastyle:off cyclomatic.complexity
  private[samplers] def hashTableRow(r: TableRow,
                                     fieldStr: String,
                                     tblSchema: Seq[TableFieldSchema],
                                     hasher: Hasher): Hasher = {
    val subfields = fieldStr.split(BigSampler.fieldSep)
    val field = tblSchema.find(_.getName == subfields.head).getOrElse {
      throw new NoSuchElementException(s"Can't find field `$fieldStr` in the schema $tblSchema")
    }
    val v = r.get(subfields.head)
    if (v == null) {
      log.debug(s"Field `${field.getName}` of type ${field.getType} and mode ${field.getMode}" +
        s" is null - won't account for hash")
      hasher
    } else {
      val vs = if (field.getMode == "REPEATED") {
        v.asInstanceOf[JList[AnyRef]].asScala
      } else {
        Seq(v)
      }
      field.getType match {
        case "BOOLEAN" => vs.foldLeft(hasher)((hasher, v) =>
          hasher.putBoolean(v.toString.toBoolean))
        case "INTEGER" => vs.foldLeft(hasher)((hasher, v) =>
          hasher.putLong(v.toString.toLong))
        case "FLOAT" => vs.foldLeft(hasher)((hasher, v) => hasher.putFloat(v.toString.toFloat))
        case "STRING" => vs.foldLeft(hasher)((hasher, v) =>
          hasher.putString(v.toString, BigSampler.utf8Charset))
        case "BYTES" => vs.foldLeft(hasher)((hasher, v) =>
          hasher.putBytes(BaseEncoding.base64().decode(v.toString)))
        case "TIMESTAMP" => vs.foldLeft(hasher)((hasher, v) =>
          hasher.putString(v.toString, BigSampler.utf8Charset))
        case "DATE" => vs.foldLeft(hasher)((hasher, v) =>
          hasher.putString(v.toString, BigSampler.utf8Charset))
        case "TIME" => vs.foldLeft(hasher)((hasher, v) =>
          hasher.putString(v.toString, BigSampler.utf8Charset))
        case "DATETIME" => vs.foldLeft(hasher)((hasher, v) =>
          hasher.putString(v.toString, BigSampler.utf8Charset))
        case "RECORD" =>
          vs.foldLeft(hasher)((hasher, vi) =>
            hashTableRow(
              vi.asInstanceOf[TableRow],
              subfields.tail.mkString(BigSampler.fieldSep.toString),
              field.getFields.asScala,
              hasher)
          )
        case t => throw new UnsupportedOperationException(
          s"Type `$t` of field `${field.getName}` is not supported as sampling key")
      }
    }
  }
  // scalastyle:on cyclomatic.complexity

  @tailrec
  private def fieldInTblSchema(tblSchema: Seq[TableFieldSchema], fieldStr: String): Boolean = {
    val subfields = fieldStr.split(BigSampler.fieldSep)
    val fieldOpt = tblSchema.find(_.getName == subfields.head)
    if (fieldOpt.isEmpty) {
      false
    } else {
      val field = fieldOpt.get
      field.getType match {
        case "RECORD" =>
          fieldInTblSchema(
            field.getFields.asScala,
            subfields.tail.mkString(BigSampler.fieldSep.toString))
        case "BOOLEAN" | "INTEGER" | "FLOAT" | "STRING" | "BYTES" => true
        case  t => throw new UnsupportedOperationException(
          s"Type `$t` of field `${field.getName}` is not supported as sampling key")
      }
    }
  }

  // TODO: investiage if possible to move this logic to BQ itself
  def sampleBigQueryTable(sc: ScioContext,
                          inputTbl: TableReference,
                          outputTbl: TableReference,
                          fields: List[String],
                          samplePct: Float,
                          seed: Option[Int]): Future[Tap[TableRow]] = {
    import BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED
    import BigQueryIO.Write.WriteDisposition.WRITE_EMPTY

    val patchedBigQueryService = new BigQueryServicesImpl()
      .getDatasetService(sc.optionsAs[BigQueryOptions])
    if (patchedBigQueryService.getTable(outputTbl) != null) {
      log.info(s"Reuse previous sample at $outputTbl")
      Taps().bigQueryTable(outputTbl)
    } else {
      log.info(s"Will sample from BigQuery table: $inputTbl, output will be $outputTbl")
      val schema = patchedBigQueryService.getTable(inputTbl).getSchema

      if (fields.isEmpty) {
        val r = sc.bigQueryTable(inputTbl)
          .sample(withReplacement = false, samplePct)
          .saveAsBigQuery(outputTbl, schema, WRITE_EMPTY, CREATE_IF_NEEDED, tableDescription = "")
        sc.close().waitUntilDone()
        r
      } else {
        val fieldsMissing = fields.filterNot(f => fieldInTblSchema(schema.getFields.asScala, f))
        if (fieldsMissing.nonEmpty) {
          throw new NoSuchElementException(
            s"""Could not locate fields ${fieldsMissing.mkString(",")}""" +
              s""" in table $inputTbl with schema $schema""")
        }
        // defeat closure TableSchema and TableFieldSchema are not serializable
        val schemaStr = JsonSerDe.toJsonString(schema)
        @transient lazy val schemaFields =
          JsonSerDe.fromJsonString(schemaStr, classOf[TableSchema]).getFields.asScala
        val samplePct100 = samplePct * 100.0
        val r = sc.bigQueryTable(inputTbl)
          .flatMap { e =>
            val hasher = BigSampler.kissHashFun(seed).newHasher(fields.size)
            val hash = fields.foldLeft(hasher)((h, f) => hashTableRow(e, f, schemaFields, h)).hash()
            BigSampler.diceElement(e, hash, samplePct100)
          }
          .saveAsBigQuery(outputTbl, schema, WRITE_EMPTY, CREATE_IF_NEEDED, tableDescription = "")
        sc.close().waitUntilDone()
        r
      }
    }
  }
}

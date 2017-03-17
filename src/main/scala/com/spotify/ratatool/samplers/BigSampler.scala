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

import com.google.api.client.googleapis.services.AbstractGoogleClientRequest
import com.google.api.client.util.{BackOff, BackOffUtils, Sleeper}
import com.google.api.services.bigquery.model.{Table, TableFieldSchema, TableReference, TableSchema}
import com.google.cloud.dataflow.sdk.io.BigQueryIO
import com.google.cloud.dataflow.sdk.options.BigQueryOptions
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction
import com.google.cloud.dataflow.sdk.util.{FluentBackoff, Transport}
import com.google.cloud.hadoop.util.ApiErrorExtractor
import com.google.common.hash.{HashCode, HashFunction, Hasher, Hashing}
import com.google.common.io.BaseEncoding
import com.spotify.ratatool.io.{AvroIO, FileStorage}
import com.spotify.ratatool.serde.JsonSerDe
import com.spotify.scio.bigquery.TableRow
import com.spotify.scio.io.{Tap, Taps}
import com.spotify.scio.{ContextAndArgs, ScioContext, _}
import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericRecord
import org.joda.time.Duration
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.language.existentials
import scala.util.{Failure, Success, Try}


object BigSampler {

  private val log = LoggerFactory.getLogger(BigSampler.getClass)
  private val utf8Charset = Charset.forName("UTF-8")

  private val fieldSep = '.'

  private def kissHashFun(seed: Option[Int] = None): HashFunction = {
    seed match {
      case Some(s) => Hashing.murmur3_128(s)
      case None => Hashing.murmur3_128(System.currentTimeMillis.toInt)
    }
  }

  private def diceElement[T](e: T,
                             hash: HashCode,
                             sampleSize: Int,
                             module: Int): Option[T] = {
    //TODO: for now leave it up to jit/compiler to optimize
    if (math.abs(hash.padToLong) % module < sampleSize) {
      Some(e)
    } else {
      None
    }
  }

  private def parseAsBigQueryTable(tblRef: String): Option[TableReference] = {
    Try(BigQueryIO.parseTableSpec(tblRef)).toOption
  }

  private def parseAsURI(uri: String): Option[URI] = {
    Try(new URI(uri)).toOption
  }

  @tailrec
  private def samplePctToInt(samplePct: Float, n: Int): (Float, Int) = {
    if (samplePct < 1.0) {
      samplePctToInt(samplePct * 10, n * 10)
    } else {
      (samplePct, n)
    }
  }

  @tailrec
  private def hashTableRow(r: TableRow,
                                 fieldStr: String,
                                 tblSchema: Seq[TableFieldSchema],
                                 hasher: Hasher): Hasher = {
    val subfields = fieldStr.split(fieldSep)
    val field = tblSchema.find(_.getName == subfields.head).getOrElse {
      throw new NoSuchElementException(s"Can't find field $fieldStr in table schema $tblSchema")
    }
    val v = r.get(subfields.head)
    if (v == null) {
      log.debug(s"Field `${field.getName}` of type ${field.getType} and mode ${field.getMode}" +
        s" is null - won't account for hash")
      hasher
    } else {
      if (field.getMode == "REPEATED") {
        throw new UnsupportedOperationException(
          s"Repeated field (`${field.getName}`) is not supported as sampling key!")
      }
      val vs = v.toString
      field.getType match {
        case "BOOLEAN" => hasher.putBoolean(vs.toBoolean)
        case "INTEGER" => hasher.putLong(vs.toLong)
        case "FLOAT" => hasher.putFloat(vs.toFloat)
        case "STRING" => hasher.putString(vs, utf8Charset)
        case "BYTES" => hasher.putBytes(BaseEncoding.base64().decode(vs))
        case "RECORD" =>
          hashTableRow(
            v.asInstanceOf[TableRow],
            subfields.tail.mkString(fieldSep.toString),
            field.getFields.asScala,
            hasher)
        // "TIMESTAMP" | "DATE" | "TIME" | "DATETIME :
        case t => throw new UnsupportedOperationException(
          s"Type `$t` of field `${field.getName}` is not supported as sampling key")
      }
    }
  }

  @tailrec
  private def fieldInTblSchema(tblSchema: Seq[TableFieldSchema], fieldStr: String): Boolean = {
    val subfields = fieldStr.split(fieldSep)
    val fieldOpt = tblSchema.find(_.getName == subfields.head)
    if (fieldOpt.isEmpty) {
      false
    } else {
      val field = fieldOpt.get
      field.getType match {
        case "RECORD" =>
          fieldInTblSchema(field.getFields.asScala, subfields.tail.mkString(fieldSep.toString))
        case "BOOLEAN" | "INTEGER" | "FLOAT" | "STRING" | "BYTES" => true
        case  t => throw new UnsupportedOperationException(
          s"Type `$t` of field `${field.getName}` is not supported as sampling key")
      }
    }
  }

  // TODO: investiage if possible to move this logic to BQ itself
  private def sampleBigQueryTable(sc: ScioContext,
                                  inputTbl: TableReference,
                                  outputTbl: TableReference,
                                  fields: List[String],
                                  samplePct: Float,
                                  seed: Option[Int]): Future[Tap[TableRow]] = {
    val patchedBigQueryService = new PatchedBigQueryService(sc.optionsAs[BigQueryOptions])

    if (patchedBigQueryService.getTable(outputTbl) != null) {
      log.info(s"Reuse previous sample at $outputTbl")
      Taps().bigQueryTable(outputTbl)
    } else {
      log.info(s"Will sample from BigQuery table: $inputTbl, output will be $outputTbl")

      val schema = patchedBigQueryService.getTable(inputTbl).getSchema
      val fieldsMissing = fields.filterNot(f => fieldInTblSchema(schema.getFields.asScala, f))
      if(fieldsMissing.nonEmpty) {
        throw new NoSuchElementException(
          s"""Could not locate fields ${fieldsMissing.mkString(",")}""" +
            s""" in table $inputTbl with schema $schema""")
      }
      // defeat closure TableSchema and TableFieldSchema are not serializable
      val schemaStr = JsonSerDe.toJsonString(schema)
      @transient lazy val schemaFields =
        JsonSerDe.fromJsonString(schemaStr, classOf[TableSchema]).getFields.asScala
      val (s, n) = samplePctToInt(samplePct, 100)
      val r = sc.bigQueryTable(inputTbl)
        .flatMap { e =>
          val hasher = kissHashFun(seed).newHasher(fields.size)
          val hash = fields.foldLeft(hasher)((h, f) => hashTableRow(e, f, schemaFields, h)).hash()
          diceElement(e, hash, s.toInt, n)
        }
        .saveAsBigQuery(outputTbl, schema, WRITE_EMPTY, CREATE_IF_NEEDED)
      sc.close()
      r
    }
  }

  // scalastyle:off cyclomatic.complexity
  @tailrec
  private def hashAvroField(r: GenericRecord,
                            fieldStr: String,
                            schema: Schema,
                            hasher: Hasher): Hasher = {
    val recordSchema = getRecordSchema(schema)
    val subfields = fieldStr.split(fieldSep)
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
          hashAvroField(r, subfields.mkString(fieldSep.toString), field.schema, hasher)
        case Type.ENUM => hasher.putString(v.asInstanceOf[Enum[_]].name, utf8Charset)
        case Type.STRING => hasher.putString(v.asInstanceOf[CharSequence], utf8Charset)
        case Type.BYTES => hasher.putBytes(v.asInstanceOf[Array[Byte]])
        case Type.INT => hasher.putInt(v.asInstanceOf[Int])
        case Type.LONG => hasher.putLong(v.asInstanceOf[Long])
        case Type.FLOAT => hasher.putFloat(v.asInstanceOf[Float])
        case Type.DOUBLE => hasher.putDouble(v.asInstanceOf[Double])
        case Type.BOOLEAN => hasher.putBoolean(v.asInstanceOf[Boolean])
        // Type.ARRAY | Type.MAP | Type.UNION | Type.FIXED | Type.NULL =>
        case t => throw new UnsupportedOperationException(
          s"Type `${field.schema.getType}` of `${field.name}` is not supported as sampling key!")
      }
    }
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
    val subfields = fieldStr.split(fieldSep)
    val fieldOpt = recordSchema.getFields.asScala.find(_.name == subfields.head)
    if (fieldOpt.isEmpty) {
      false
    } else {
      val field = fieldOpt.get
      field.schema.getType match {
        case Type.RECORD =>
          fieldInAvroSchema(field.schema, subfields.tail.mkString(fieldSep.toString))
        case Type.ENUM | Type.STRING | Type.BYTES | Type.INT | Type.LONG| Type.FLOAT | Type.DOUBLE
             | Type.BOOLEAN => true
        case  t => throw new UnsupportedOperationException(
          s"Type `${field.schema.getType}` of `${field.name}` is not supported as sampling key!")
      }
    }
  }

  private def sampleAvro(sc: ScioContext,
                         input: String,
                         output: String,
                         fields: Seq[String],
                         samplePct: Float,
                         seed: Option[Int]): Future[Tap[GenericRecord]] = {
    val schema = AvroIO.getAvroSchemaFromFile(input.toString)
    val outputParts =
      if (output.endsWith("/")) output + "part*" else output + "/part*"
    if (FileStorage(outputParts).isDone) {
      log.info(s"Reuse previous sample at $outputParts")
      Taps().avroFile(outputParts, schema = schema)
    } else {
      log.info(s"Will sample from: $input, output will be $output")
      val (s, n) = samplePctToInt(samplePct, 100)
      val fieldsMissing = fields.filterNot(f => fieldInAvroSchema(schema, f))
      if(fieldsMissing.nonEmpty) {
        throw new NoSuchElementException(
          s"""Could not locate field(s) ${fieldsMissing.mkString(",")} """ +
            s"""in table $input with schema $schema""")
      }
      val schemaSer = schema.toString(false)
      @transient lazy val schemaSerDe = new Schema.Parser().parse(schemaSer)
      val r = sc.avroFile[GenericRecord](input, schema)
        .flatMap { e =>
          val hasher = kissHashFun(seed).newHasher(fields.size)
          val hash = fields.foldLeft(hasher)((h, f) => hashAvroField(e, f, schemaSerDe, h)).hash
          diceElement(e, hash, s.toInt, n)
        }
        .saveAsAvroFile(output, schema = schema)
      sc.close()
      r
    }
  }

  def singleInput(argv: Array[String]): Future[Tap[_]] = {
    val (sc, args) = ContextAndArgs(argv)
    val samplePct = args("sample").toFloat

    require(samplePct > 0.0F && samplePct < 100.0F,
      "Sample percentage should be between (0.0, 100.0)")

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
        s"Input is a BigQuery table `${input}`, output should be a BigQuery table too," +
          s"but instead it's `${output}`.")
      val inputTbl = parseAsBigQueryTable(input).get
      val outputTbl = parseAsBigQueryTable(output).get
      sampleBigQueryTable(sc, inputTbl, outputTbl, fields, samplePct, seed.map(_.toInt))
    } else if (parseAsURI(input).isDefined) {
      // right now only support for avro
      require(parseAsURI(output).isDefined,
        s"Input is a URI: `${input}`, output should be a URI too, but instead it's `${output}`.")
      sampleAvro(sc, input, output, fields, samplePct, seed.map(_.toInt))
    } else {
      throw new UnsupportedOperationException(s"Input `${input} not supported.")
    }
  }

  def main(argv: Array[String]): Unit = {
    this.singleInput(argv)
  }
}

/**
 * Patched BigQuery Service without bug which retries request if table does not exist.
 */
private[ratatool] class PatchedBigQueryService(options: BigQueryOptions) {
  private val log = LoggerFactory.getLogger(classOf[PatchedBigQueryService])
  private val client = Transport.newBigQueryClient(options).build

  def getTable(tbl: TableReference): Table = {
    val retires = 9
    val backoff =
      FluentBackoff.DEFAULT
        .withMaxRetries(retires).withInitialBackoff(Duration.standardSeconds(1)).backoff
    try {
      executeWithRetries(
        client.tables.get(tbl.getProjectId, tbl.getDatasetId, tbl.getTableId),
        s"Unable to get table: ${tbl.getTableId}, aborting after $retires retries.",
        Sleeper.DEFAULT,
        backoff,
        new SerializableFunction[IOException, Boolean] {
          override def apply(input: IOException): Boolean = {
            val extractor = new ApiErrorExtractor()
            !extractor.itemNotFound(input)
          }
        })
    } catch {
      case e: IOException => {
        val extractor = new ApiErrorExtractor()
        if (extractor.itemNotFound(e)) {
          null
        } else {
          throw e
        }
      }
    }
  }

  def executeWithRetries[T](request: AbstractGoogleClientRequest[T],
                            errorMessage: String,
                            sleeper: Sleeper,
                            backoff: BackOff,
                            shouldRetry: SerializableFunction[IOException, Boolean]): T = {
    var lastException: IOException = null
    var r: T = null.asInstanceOf[T]
    do {
      if (lastException != null) {
        log.info("Ignore the error and retry the request.", lastException)
      }
      Try(request.execute()) match {
        case Success(t) => r = t
        case Failure(e) => lastException = e.asInstanceOf[IOException]
      }
    } while (r != null && !shouldRetry.apply(lastException) && nextBackOff(sleeper, backoff))
    if (r != null) {
      r
    } else {
      throw new IOException(errorMessage, lastException)
    }
  }

  private def nextBackOff(sleeper: Sleeper, backoff: BackOff): Boolean = {
    try {
      BackOffUtils.next(sleeper, backoff)
    } catch {
      case e: IOException => throw new RuntimeException(e)
    }
  }
}

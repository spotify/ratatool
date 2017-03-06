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
import com.google.protobuf.ByteString
import com.spotify.ratatool.io.{AvroIO, FileStorage}
import com.spotify.ratatool.serde.JsonSerDe
import com.spotify.scio.bigquery.TableRow
import com.spotify.scio.io.{Tap, Taps}
import com.spotify.scio.{ContextAndArgs, ScioContext, _}
import org.apache.avro.Schema
import org.apache.avro.Schema.{Field, Type}
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

  private val kissHashFun = Hashing.murmur3_128(System.currentTimeMillis.toInt)
  private def kissHashFun(seed: Option[Int]): HashFunction = {
    seed match {
      case Some(s) =>  Hashing.murmur3_128(s)
      case None => kissHashFun
    }
  }

  private case class PathsAndSchemas(input: URI,
                                     output: URI,
                                     schema: Schema,
                                     sampleSchema: Class[_])

  // scalastyle:off cyclomatic.complexity
  private def hashAvroField(r: GenericRecord, fields: Seq[Field], hasher: Hasher): Hasher = {
    fields.foldLeft(hasher) { case (h, field) =>
      //TODO: is there need to check for nulls in v?
      val v = r.get(field.name())
      field.schema().getType match {
        case Type.RECORD => hashAvroField(r, field.schema().getFields.asScala, h)
        case Type.ENUM => h.putString(v.asInstanceOf[Enum[_]].name(), utf8Charset)
        case Type.ARRAY =>
          throw new UnsupportedOperationException("Array is not supported as sampling key!")
        case Type.MAP =>
          throw new UnsupportedOperationException("Map is not supported as sampling key!")
        case Type.UNION =>
          throw new UnsupportedOperationException("Union is not supported as sampling key!")
        case Type.FIXED =>
          throw new UnsupportedOperationException("Fixed is not supported as sampling key!")
        case Type.STRING => h.putString(v.asInstanceOf[CharSequence], utf8Charset)
        case Type.BYTES => h.putBytes(v.asInstanceOf[Array[Byte]])
        case Type.INT => h.putInt(v.asInstanceOf[Int])
        case Type.LONG => h.putLong(v.asInstanceOf[Long])
        case Type.FLOAT => h.putFloat(v.asInstanceOf[Float])
        case Type.DOUBLE => h.putDouble(v.asInstanceOf[Double])
        case Type.BOOLEAN => h.putBoolean(v.asInstanceOf[Boolean])
        case Type.NULL =>
          throw new UnsupportedOperationException("Null is not supported as sampling key!")
      }
    }
  }
  // scalastyle:on cyclomatic.complexity

  private def diceElement[T](e: T,
                             hash: HashCode,
                             sampleSize: Int,
                             module: Int): TraversableOnce[T] = {
    //TODO: for now leave it up to jit/compiler to optimize
    if (math.abs(hash.padToLong()) % module < sampleSize) {
      Iterable(e)
    } else {
      Iterable.empty
    }
  }

  private def parseAsBigQueryTable(tblRef: String): Option[TableReference] = {
    Try(BigQueryIO.parseTableSpec(tblRef)).toOption
  }

  private def parseAsURI(uri: String): Option[URI] = {
    Try(new URI(uri)).toOption
  }

  // scalastyle:off cyclomatic.complexity
  private def hashTableRow(r: TableRow,
                           tblSchema: List[TableFieldSchema],
                           fields: Seq[String],
                           hasher: Hasher): Hasher = {
    //TODO: do we need to check for nulls
    fields.foldLeft(hasher) { case (h, f) =>
      val fieldParts = f.split('.')
      val fieldOpt = tblSchema.find(_.getName == fieldParts.head)
      if (fieldOpt.isEmpty) {
        log.warn(s"Can't find field ${fieldParts.head} in table schema")
        hasher
      } else {
        val v = r.get(fieldParts.head)
        val field = fieldOpt.get
        if (field.getMode == "REPEATED") {
          //TODO: support repeated fields
          throw new UnsupportedOperationException(
            "Repeated field is not supported as sampling key!")
        }
        field.getType match {
          case "BOOLEAN" => h.putBoolean(v.asInstanceOf[Boolean])
          case "INTEGER" => h.putLong(v.asInstanceOf[Long])
          case "FLOAT" => h.putFloat(v.asInstanceOf[Float])
          case "STRING" => h.putString(v.asInstanceOf[String], utf8Charset)
          case "BYTES" => h.putBytes(v.asInstanceOf[ByteString].toByteArray)
          case "TIMESTAMP" =>
            throw new UnsupportedOperationException("Timestamp is not supported as sampling key!")
          case "DATE" =>
            throw new UnsupportedOperationException("Date is not supported as sampling key!")
          case "TIME" =>
            throw new UnsupportedOperationException("Time is not supported as sampling key!")
          case "DATETIME" =>
            throw new UnsupportedOperationException("Datetime is not supported as sampling key!")
          case "RECORD" =>
            hashTableRow(
              v.asInstanceOf[TableRow],
              field.getFields.asScala.toList,
              fieldParts.tail,
              h)
          case t => throw new IllegalArgumentException(s"Type: $t not supported")
        }
      }
    }
  }
  // scalastyle:on cyclomatic.complexity

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
      log.info(s"Will sample from BigQuery table: ${inputTbl}, output will be ${outputTbl}")

      val schema = patchedBigQueryService.getTable(inputTbl).getSchema

      // defeat closure TableSchema and TableFieldSchema are not serializable
      val schemaStr = JsonSerDe.toJsonString(schema)
      @transient lazy val schemaFields =
        JsonSerDe.fromJsonString(schemaStr, classOf[TableSchema]).getFields.asScala.toList
      val (s, n) = samplePctToInt(samplePct, 100)
      val r = sc.bigQueryTable(inputTbl)
        .flatMap { e =>
          val hash = hashTableRow(
            e,
            schemaFields,
            fields,
            kissHashFun(seed).newHasher()).hash()
          diceElement(e, hash, s.toInt, n)
        }
        .saveAsBigQuery(outputTbl, schema, WRITE_EMPTY, CREATE_IF_NEEDED)
      sc.close()
      r
    }
  }

  private def sampleAvro(sc: ScioContext,
                         input: String,
                         output: String,
                         sampleClass: Class[_],
                         samplePct: Float,
                         seed: Option[Int]): Future[Tap[GenericRecord]] = {
    val schema = AvroIO.getAvroSchemaFromFile(input.toString)
    val outputParts =
      if (output.endsWith("/")) output + "part*" else output + "/part*"
    if (FileStorage(outputParts).isDone) {
      // sampling done previously - just reuse
      log.info(s"Reuse previous sample at $outputParts")
      Taps().avroFile(outputParts, schema = schema)
    } else {
      log.info(s"Will sample from: $input, output will be $output")
      @transient lazy val sampleSchema = sampleClass.
        asInstanceOf[Class[GenericRecord]].newInstance().getSchema
      val (s, n) = samplePctToInt(samplePct, 100)
      val r = sc.avroFile[GenericRecord](input, schema)
        .flatMap { e =>
          val hash =
            hashAvroField(e, sampleSchema.getFields.asScala, kissHashFun(seed).newHasher()).hash()
          diceElement(e, hash, s.toInt, n)
        }
        .saveAsAvroFile(output, schema = schema)
      sc.close()
      r
    }
  }

  @tailrec
  private def samplePctToInt(samplePct: Float, n: Int): (Float, Int) = {
    if (samplePct < 1.0) {
      samplePctToInt(samplePct * 10, n * 10)
    } else {
      (samplePct, n)
    }
  }

  def singleInput(argv: Array[String]): Future[Tap[_]] = {
    val (sc, args) = ContextAndArgs(argv)
    val samplePct = args("sample").toFloat
    require(samplePct > 0.0F && samplePct < 100.0F,
      "Sample percentage should be between (0.0, 100.0)")

    val input = args("input")
    val output = args("output")
    val sampleAvroSchemaClass = args.optional("avro-schema")
    val fields = args.list("fields")
    val seed = args.optional("seed")

    if (parseAsBigQueryTable(input).isDefined) {
      require(parseAsBigQueryTable(output).isDefined,
        s"Input is a BigQuery table `${input}`, output should be a BigQuery table too," +
          s"but instead it's `${output}`.")
      require(fields.nonEmpty && sampleAvroSchemaClass.isEmpty,
        "Input is a BigQuery table, please specify fields to sample on, via the --fields option.")
      val inputTbl = parseAsBigQueryTable(input).get
      val outputTbl = parseAsBigQueryTable(output).get
      sampleBigQueryTable(sc, inputTbl, outputTbl, fields, samplePct, seed.map(_.toInt))
    } else if (parseAsURI(input).isDefined) {
      // right now only support for avro
      require(parseAsURI(output).isDefined,
        s"Input is a URI: `${input}`, output should be a URI too, but instead it's `${output}`.")
      require(fields.isEmpty && sampleAvroSchemaClass.isDefined,
        "Input is a URI, please specify sample schema via the --avro-schema option.")
      val sampleClass = Class.forName(sampleAvroSchemaClass.get)
      sampleAvro(sc, input, output, sampleClass, samplePct, seed.map(_.toInt))
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
class PatchedBigQueryService(options: BigQueryOptions) {
  val log = LoggerFactory.getLogger(classOf[PatchedBigQueryService])
  val client = Transport.newBigQueryClient(options).build

  def getTable(tbl: TableReference): Table = {
    val retires = 9
    val backoff =
      FluentBackoff.DEFAULT
        .withMaxRetries(retires).withInitialBackoff(Duration.standardSeconds(1)).backoff()
    try {
      executeWithRetries(
        client.tables().get(tbl.getProjectId, tbl.getDatasetId, tbl.getTableId),
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

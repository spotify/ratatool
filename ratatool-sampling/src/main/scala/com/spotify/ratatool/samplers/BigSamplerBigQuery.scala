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

import java.util.{List => JList}

import com.google.api.services.bigquery.model.{TableFieldSchema, TableReference}
import com.google.common.hash.Hasher
import com.spotify.ratatool.samplers.util.{ByteEncoding, Precision, RawEncoding, SampleDistribution}
import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery.TableRow
import com.spotify.scio.io.{Tap, Taps}
import org.apache.beam.sdk.io.gcp.bigquery.{BigQueryIO, BigQueryOptions,
  PatchedBigQueryServicesImpl}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.annotation.tailrec
import scala.concurrent.Future
import com.spotify.scio.bigquery._
import com.spotify.scio.bigquery.client.BigQuery

private[samplers] object BigSamplerBigQuery {

  private val log = LoggerFactory.getLogger(BigSamplerBigQuery.getClass)

  // scalastyle:off cyclomatic.complexity
  private[samplers] def hashTableRow(tblSchema: => Seq[TableFieldSchema])
                                    (r: TableRow, fieldStr: String, hasher: Hasher)
  : Hasher = {
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
          hasher.putBytes(v.asInstanceOf[Array[Byte]]))
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
            hashTableRow(field.getFields.asScala)(
              vi.asInstanceOf[TableRow],
              subfields.tail.mkString(BigSampler.fieldSep.toString),
              hasher)
          )
        case t => throw new UnsupportedOperationException(
          s"Type `$t` of field `${field.getName}` is not supported as sampling key")
      }
    }
  }
  // scalastyle:on cyclomatic.complexity

  // scalastyle:off cyclomatic.complexity
  // TODO: Potentially reduce this and hashAvroField to a single function
  @tailrec
  private[samplers] def getTableRowField(r: TableRow,
                                         fieldStr: String,
                                         tblSchema: Seq[TableFieldSchema]): Any = {
    val subfields = fieldStr.split(BigSampler.fieldSep)
    val field = tblSchema.find(_.getName == subfields.head).getOrElse {
      throw new NoSuchElementException(s"Can't find field `$fieldStr` in the schema $tblSchema")
    }
    val v = r.get(subfields.head)
    if (v == null) {
      log.debug(s"Field `${field.getName}` of type ${field.getType} and mode ${field.getMode}" +
        s" is null - won't account for hash")
    } else {
      field.getType match {
        case "RECORD" if fieldStr.nonEmpty =>
          // v is a LinkedHashMap
          getTableRowField(
            TableRow(v.asInstanceOf[java.util.Map[String, AnyRef]].asScala.toList: _*),
            subfields.tail.mkString(BigSampler.fieldSep.toString),
            field.getFields.asScala)
        case _ => v
      }
    }
  }
  // scalastyle:on cyclomatic.complexity

  private[samplers] def buildKey(schema: => Seq[TableFieldSchema],
                                 distributionFields: Seq[String])(tr: TableRow)
  : Set[Any] = {
    distributionFields.map{ f =>
      getTableRowField(tr, f, schema)
    }.toSet
  }

  //scalastyle:off method.length cyclomatic.complexity parameter.number
  // TODO: investigate if possible to move this logic to BQ itself
  private[samplers] def sample(sc: ScioContext,
                               inputTbl: TableReference,
                               outputTbl: TableReference,
                               fields: List[String],
                               fraction: Double,
                               seed: Option[Int],
                               distribution: Option[SampleDistribution],
                               distributionFields: List[String],
                               precision: Precision,
                               sizePerKey: Int,
                               byteEncoding: ByteEncoding = RawEncoding)
  : Future[Tap[TableRow]] = {
    import BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED
    import BigQueryIO.Write.WriteDisposition.WRITE_EMPTY

    val patchedBigQueryService = new PatchedBigQueryServicesImpl()
      .getDatasetService(sc.optionsAs[BigQueryOptions])
    if (patchedBigQueryService.tables.table(outputTbl) != null) {
      log.info(s"Reuse previous sample at $outputTbl")
      Taps().bigQueryTable(outputTbl)
    } else {
      log.info(s"Will sample from BigQuery table: $inputTbl, output will be $outputTbl")
      val schema = patchedBigQueryService.tables.table(inputTbl).getSchema

      val coll = sc.bigQueryTable(inputTbl)

      val sampledCollection = sampleTableRow(coll, fraction, schema, fields, seed, distribution,
        distributionFields, precision, sizePerKey, byteEncoding)

      val r = sampledCollection
        .saveAsBigQuery(outputTbl, schema, WRITE_EMPTY, CREATE_IF_NEEDED, tableDescription = "")
      sc.close().waitUntilDone()
      r
    }
  }
  //scalastyle:on method.length cyclomatic.complexity parameter.number
}

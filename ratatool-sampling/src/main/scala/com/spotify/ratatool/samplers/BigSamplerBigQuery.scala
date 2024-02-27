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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.Hasher
import com.spotify.ratatool.samplers.util._
import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery.TableRow
import com.spotify.scio.io.ClosedTap
import org.apache.beam.sdk.io.gcp.bigquery.{
  BigQueryIO,
  BigQueryOptions,
  PatchedBigQueryServicesImpl
}
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._
import scala.annotation.tailrec
import com.spotify.scio.bigquery._

private[samplers] object BigSamplerBigQuery {

  private val log = LoggerFactory.getLogger(BigSamplerBigQuery.getClass)

  private[samplers] def hashTableRow(
    tblSchema: => Seq[TableFieldSchema]
  )(r: TableRow, fieldStr: String, hasher: Hasher): Hasher = {
    val subfields = fieldStr.split(BigSampler.fieldSep)
    val field = tblSchema.find(_.getName == subfields.head).getOrElse {
      throw new NoSuchElementException(s"Can't find field `$fieldStr` in the schema $tblSchema")
    }
    val v = r.get(subfields.head)
    if (v == null) {
      log.debug(
        s"Field `${field.getName}` of type ${field.getType} and mode ${field.getMode}" +
          s" is null - won't account for hash"
      )
      hasher
    } else {
      val vs = if (field.getMode == "REPEATED") {
        v.asInstanceOf[JList[AnyRef]].asScala
      } else {
        Seq(v)
      }
      field.getType match {
        case "BOOLEAN" =>
          vs.foldLeft(hasher)((hasher, v) => hasher.putBoolean(v.toString.toBoolean))
        case "INTEGER" => vs.foldLeft(hasher)((hasher, v) => hasher.putLong(v.toString.toLong))
        case "FLOAT"   => vs.foldLeft(hasher)((hasher, v) => hasher.putFloat(v.toString.toFloat))
        case "STRING" =>
          vs.foldLeft(hasher)((hasher, v) => hasher.putString(v.toString, BigSampler.utf8Charset))
        case "BYTES" =>
          vs.foldLeft(hasher)((hasher, v) => hasher.putBytes(v.asInstanceOf[Array[Byte]]))
        case "TIMESTAMP" =>
          vs.foldLeft(hasher)((hasher, v) => hasher.putString(v.toString, BigSampler.utf8Charset))
        case "DATE" =>
          vs.foldLeft(hasher)((hasher, v) => hasher.putString(v.toString, BigSampler.utf8Charset))
        case "TIME" =>
          vs.foldLeft(hasher)((hasher, v) => hasher.putString(v.toString, BigSampler.utf8Charset))
        case "DATETIME" =>
          vs.foldLeft(hasher)((hasher, v) => hasher.putString(v.toString, BigSampler.utf8Charset))
        case "RECORD" =>
          vs.foldLeft(hasher)((hasher, vi) =>
            hashTableRow(field.getFields.asScala.toList)(
              TableRow(vi.asInstanceOf[java.util.Map[String, Any]].asScala.toList: _*),
              subfields.tail.mkString(BigSampler.fieldSep.toString),
              hasher
            )
          )
        case t =>
          throw new UnsupportedOperationException(
            s"Type `$t` of field `${field.getName}` is not supported as sampling key"
          )
      }
    }
  }

  // TODO: Potentially reduce this and hashAvroField to a single function
  @tailrec
  private[samplers] def getTableRowField(
    r: TableRow,
    fieldStr: String,
    tblSchema: Seq[TableFieldSchema]
  ): Any = {
    val subfields = fieldStr.split(BigSampler.fieldSep)
    val field = tblSchema.find(_.getName == subfields.head).getOrElse {
      throw new NoSuchElementException(s"Can't find field `$fieldStr` in the schema $tblSchema")
    }
    val v = r.get(subfields.head)
    if (v == null) {
      log.debug(
        s"Field `${field.getName}` of type ${field.getType} and mode ${field.getMode}" +
          s" is null - won't account for hash"
      )
    } else {
      field.getType match {
        case "RECORD" if fieldStr.nonEmpty =>
          // v is a LinkedHashMap
          getTableRowField(
            TableRow(v.asInstanceOf[java.util.Map[String, AnyRef]].asScala.toList: _*),
            subfields.tail.mkString(BigSampler.fieldSep.toString),
            field.getFields.asScala.toList
          )
        case _ => v
      }
    }
  }

  /**
   * Builds a key function per record Sets do not have deterministic ordering so we return a sorted
   * list
   */
  private[samplers] def buildKey(schema: => Seq[TableFieldSchema], distributionFields: Seq[String])(
    tr: TableRow
  ): List[String] = {
    distributionFields
      .map { f =>
        getTableRowField(tr, f, schema)
      }
      .map(_.toString)
      .toSet
      .toList
      .sorted
  }

  // TODO: investigate if possible to move this logic to BQ itself
  private[samplers] def sample(
    sc: ScioContext,
    inputTbl: TableReference,
    outputTbl: TableReference,
    fields: List[String],
    fraction: Double,
    seed: Option[Int],
    hashAlgorithm: HashAlgorithm,
    distribution: Option[SampleDistribution],
    distributionFields: List[String],
    precision: Precision,
    sizePerKey: Int,
    byteEncoding: ByteEncoding = RawEncoding,
    bigqueryPartitioning: String
  ): ClosedTap[TableRow] = {
    import BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED
    import BigQueryIO.Write.WriteDisposition.WRITE_EMPTY

    val patchedBigQueryService = new PatchedBigQueryServicesImpl()
      .getDatasetService(sc.optionsAs[BigQueryOptions])
    if (patchedBigQueryService.getTable(outputTbl) != null) {
      log.info(s"Reuse previous sample at $outputTbl")
      ClosedTap(BigQueryTap(outputTbl))
    } else {
      log.info(s"Will sample from BigQuery table: $inputTbl, output will be $outputTbl")
      val schema = patchedBigQueryService.getTable(inputTbl).getSchema

      val coll = sc.bigQueryTable(Table.Ref(inputTbl))

      val sampledCollection = sampleTableRow(
        coll,
        fraction,
        schema,
        fields,
        seed,
        hashAlgorithm,
        distribution,
        distributionFields,
        precision,
        sizePerKey,
        byteEncoding
      )

      val partitioning = bigqueryPartitioning match {
        case "NULL" => null
        case _      => TimePartitioning(bigqueryPartitioning)
      }
      val r = sampledCollection
        .saveAsBigQueryTable(
          Table.Ref(outputTbl),
          schema,
          WRITE_EMPTY,
          CREATE_IF_NEEDED,
          tableDescription = "",
          partitioning
        )
      sc.run().waitUntilDone()
      r
    }
  }
}

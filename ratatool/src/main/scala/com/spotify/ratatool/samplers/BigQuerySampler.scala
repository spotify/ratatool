/*
 * Copyright 2016 Spotify AB.
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

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.util.Utils
import com.google.api.services.bigquery.model.{TableReference, TableRow, TableSchema}
import com.google.api.services.bigquery.{Bigquery, BigqueryScopes}
import org.apache.beam.sdk.io.gcp.bigquery.{BigQueryIO, BigQueryTableRowIterator}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 * Sampler for BigQuery tables.
 *
 * Only head mode is supported.
 */
class BigQuerySampler(tableRef: TableReference, protected val seed: Option[Long] = None)
  extends Sampler[TableRow]{

  private val logger: Logger = LoggerFactory.getLogger(classOf[BigQuerySampler])

  private val bigquery: Bigquery = {
    val scopes = List(BigqueryScopes.BIGQUERY).asJava
    val credential = GoogleCredential.getApplicationDefault.createScoped(scopes)
    new Bigquery.Builder(Utils.getDefaultTransport, Utils.getDefaultJsonFactory, credential)
      .setApplicationName("sampler")
      .build()
  }

  private lazy val table = bigquery
    .tables()
    .get(tableRef.getProjectId, tableRef.getDatasetId, tableRef.getTableId)
    .execute()

  override def sample(n: Long, head: Boolean): Seq[TableRow] = {
    require(n > 0, "n must be > 0")
    require(head, "BigQuery can only be used with --head")
    logger.info("Taking a sample of {} from BigQuery table {}", n, BigQueryIO.toTableSpec(tableRef))

    val numRows = BigInt(table.getNumRows)

    val iterator = BigQueryTableRowIterator.fromTable(tableRef, bigquery)
    iterator.open()
    val result = ListBuffer.empty[TableRow]
    while (result.length < (numRows min n) && iterator.advance()) {
      result.append(iterator.getCurrent)
    }
    result
  }

  def schema: TableSchema = table.getSchema

}

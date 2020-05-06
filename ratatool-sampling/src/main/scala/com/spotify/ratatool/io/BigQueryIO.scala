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

package com.spotify.ratatool.io

import java.util

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.util.Utils
import com.google.api.services.bigquery.model.{Table, TableReference, TableRow, TableSchema}
import com.google.api.services.bigquery.{Bigquery, BigqueryScopes}
import org.apache.beam.sdk.io.gcp.bigquery.{BigQueryOptions,
                                            PatchedBigQueryServicesImpl,
                                            InsertRetryPolicy,
                                            PatchedBigQueryTableRowIterator}
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.windowing.{GlobalWindow, PaneInfo}
import org.apache.beam.sdk.values.ValueInSingleWindow
import org.joda.time.Instant

import scala.jdk.CollectionConverters._
import com.spotify.scio.bigquery.client.BigQuery

/** Utilities for BigQuery IO. */
object BigQueryIO {

  /** Parse a table specification string. */
  def parseTableSpec(tableSpec: String): TableReference =
    org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.parseTableSpec(tableSpec)

  /** Convert a table reference to string. */
  def toTableSpec(tableRef: TableReference): String =
    org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.toTableSpec(tableRef)

  /** BigQuery Java client. */
  val bigquery: Bigquery = {
    val credential = GoogleCredential
      .getApplicationDefault
      .createScoped(List(BigqueryScopes.BIGQUERY).asJava)
    new Bigquery.Builder(Utils.getDefaultTransport, Utils.getDefaultJsonFactory, credential)
      .setApplicationName("ratatool")
      .build()
  }

  /** Read records from a BigQuery table. */
  def readFromTable(tableRef: TableReference): Iterator[TableRow] =
    new TableRowIterator(PatchedBigQueryTableRowIterator.fromTable(tableRef, bigquery))

  /** Read records from a BigQuery table. */
  def readFromTable(tableSpec: String): Iterator[TableRow] =
    readFromTable(parseTableSpec(tableSpec))

  /** Write records to a BigQuery table. */
  def writeToTable(data: Seq[TableRow], schema: TableSchema, tableRef: TableReference): Unit = {
    val ds = new PatchedBigQueryServicesImpl()
      .getDatasetService(PipelineOptionsFactory.create().as(classOf[BigQueryOptions]))
    val rows = data.map(e =>
      ValueInSingleWindow.of(e, Instant.now(), GlobalWindow.INSTANCE, PaneInfo.NO_FIRING))
    val failures = new java.util.ArrayList[ValueInSingleWindow[TableRow]]
    val tbl = new Table()
      .setTableReference(tableRef)
      .setSchema(schema)
    ds.createTable(tbl)
    ds.insertAll(tableRef, rows.asJava, null, InsertRetryPolicy.alwaysRetry(), failures)
  }

  /** Write records to a BigQuery table. */
  def writeToTable(data: Seq[TableRow], schema: TableSchema, tableSpec: String): Unit =
    writeToTable(data, schema, parseTableSpec(tableSpec))

}

private class TableRowIterator(private val iter: PatchedBigQueryTableRowIterator)
  extends Iterator[TableRow] {
  private var _isOpen = false
  private var _hasNext = false

  private def init(): Unit = if (!_isOpen) {
    iter.open()
    _isOpen = true
    _hasNext = iter.advance()
  }

  override def hasNext: Boolean = {
    init()
    _hasNext
  }

  override def next(): TableRow = {
    init()
    if (_hasNext) {
      val r = iter.getCurrent
      _hasNext = iter.advance()
      r
    } else {
      throw new NoSuchElementException
    }
  }
}

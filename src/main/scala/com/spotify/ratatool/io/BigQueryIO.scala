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

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.util.Utils
import com.google.api.services.bigquery.model.{TableReference, TableRow, TableSchema}
import com.google.api.services.bigquery.{Bigquery, BigqueryScopes}
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import com.google.cloud.dataflow.sdk.util.{BigQueryTableInserter, BigQueryTableRowIterator}

import scala.collection.JavaConverters._

/** Utilities for BigQuery IO. */
object BigQueryIO {

  /** Parse a table specification string. */
  def parseTableSpec(tableSpec: String): TableReference =
    com.google.cloud.dataflow.sdk.io.BigQueryIO.parseTableSpec(tableSpec)

  /** Convert a table reference to string. */
  def toTableSpec(tableRef: TableReference): String =
    com.google.cloud.dataflow.sdk.io.BigQueryIO.toTableSpec(tableRef)

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
    new TableRowIterator(BigQueryTableRowIterator.fromTable(tableRef, bigquery))

  /** Read records from a BigQuery table. */
  def readFromTable(tableSpec: String): Iterator[TableRow] =
    readFromTable(parseTableSpec(tableSpec))

  /** Write records to a BigQuery table. */
  def writeToTable(data: Seq[TableRow], schema: TableSchema, tableRef: TableReference): Unit = {
    val inserter = new BigQueryTableInserter(bigquery)
    inserter.getOrCreateTable(
      tableRef,
      WriteDisposition.WRITE_EMPTY,
      CreateDisposition.CREATE_IF_NEEDED,
      schema)
    inserter.insertAll(tableRef, data.asJava)
  }

  /** Write records to a BigQuery table. */
  def writeToTable(data: Seq[TableRow], schema: TableSchema, tableSpec: String): Unit =
    writeToTable(data, schema, parseTableSpec(tableSpec))

}

private class TableRowIterator(private val iterator: BigQueryTableRowIterator)
  extends Iterator[TableRow] {
  private var _isOpen = false
  private var _hasNext = false

  private def init(): Unit = if (!_isOpen) {
    iterator.open()
    _isOpen = true
    _hasNext = iterator.advance()
  }

  override def hasNext: Boolean = {
    init()
    _hasNext
  }

  override def next(): TableRow = {
    init()
    if (_hasNext) {
      val r = iterator.getCurrent
      _hasNext = iterator.advance()
      r
    } else {
      throw new NoSuchElementException
    }
  }
}

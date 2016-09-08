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

package com.spotify.ratatool.diffy

import java.io.StringReader

import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.google.api.client.json.JsonObjectParser
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.model.{TableFieldSchema, TableRow, TableSchema}

import scala.collection.JavaConverters._

/** Field level diff tool for TableRow records. */
class TableRowDiffy(tableSchema: TableSchema) extends Diffy[TableRow] {
  override def apply(x: TableRow, y: TableRow): Seq[Delta] =
    diff(x, y, schema.getFields.asScala, "")

  type Record = java.util.Map[String, AnyRef]

  // TableSchema is not serializable
  private val schemaString: String =
  new ObjectMapper().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
    .writeValueAsString(tableSchema)
  private lazy val schema: TableSchema =
    new JsonObjectParser(new JacksonFactory)
      .parseAndClose(new StringReader(schemaString), classOf[TableSchema])

  private def diff(x: Record, y: Record,
                   fields: Seq[TableFieldSchema], root: String): Seq[Delta] = {
    fields.flatMap { f =>
      val name = f.getName
      val fullName = if (root.isEmpty) name else root + "." + name
      if (f.getType == "RECORD" && f.getMode != "REPEATED") {
        val a = x.get(name).asInstanceOf[Record]
        val b = y.get(name).asInstanceOf[Record]
        if (a == null && b == null) {
          Nil
        } else if (a == null || b == null) {
          Seq(Delta(fullName, a, b, UnknownDelta))
        } else {
          diff(a, b, f.getFields.asScala, fullName)
        }
      } else {
        val a = x.get(name)
        val b = y.get(name)
        if (a == b) Nil else Seq(Delta(fullName, a, b, delta(a, b)))
      }
    }
  }

}

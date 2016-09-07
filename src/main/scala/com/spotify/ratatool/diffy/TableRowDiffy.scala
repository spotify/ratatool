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

import com.google.api.services.bigquery.model.{TableFieldSchema, TableRow, TableSchema}

import scala.collection.JavaConverters._

/** Field level diff tool for TableRow records. */
object TableRowDiffy {

  type Record = java.util.Map[String, AnyRef]

  /** Compare two TableRow records. */
  def apply(x: TableRow, y: TableRow, schema: TableSchema): Seq[Delta] = {
    diff(x, y, schema.getFields.asScala, "")
  }

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
          Seq(Delta(fullName, a, b))
        } else {
          diff(a, b, f.getFields.asScala, fullName)
        }
      } else {
        val a = x.get(name)
        val b = y.get(name)
        if (a == b) Nil else Seq(Delta(fullName, a, b))
      }
    }
  }

}

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

object TableRowDiffy {

  def apply(x: TableRow, y: TableRow, schema: TableSchema): Seq[Delta] = {
    diff(x, y, schema.getFields.asScala, "")
  }

  private def diff(x: TableRow, y: TableRow,
                   fields: Seq[TableFieldSchema], root: String): Seq[Delta] = {
    fields.flatMap { f =>
      val name = f.getName
      val fullName = if (root.isEmpty) name else root + "." + name
      f.getType match {
        case "RECORD" =>
          val a = x.get(name).asInstanceOf[TableRow]
          val b = y.get(name).asInstanceOf[TableRow]
          if (a != b && (a == null || b == null)) {
            Seq(Delta(fullName, a, b))
          } else {
            diff(a, b, f.getFields.asScala, fullName)
          }
        case _ =>
          val a = x.get(name)
          val b = y.get(name)
          if (a == b) Nil else Seq(Delta(fullName, a, b))
      }
    }
  }

}

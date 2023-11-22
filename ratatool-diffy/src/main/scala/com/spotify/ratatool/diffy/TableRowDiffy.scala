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
import com.google.api.client.json.gson.GsonFactory
import com.google.api.services.bigquery.model.{TableFieldSchema, TableRow, TableSchema}

import scala.jdk.CollectionConverters._
import scala.util.Try

/** Field level diff tool for TableRow records. */
class TableRowDiffy(
  tableSchema: TableSchema,
  ignore: Set[String] = Set.empty,
  unordered: Set[String] = Set.empty,
  unorderedFieldKeys: Map[String, String] = Map()
) extends Diffy[TableRow](ignore, unordered, unorderedFieldKeys) {

  override def apply(x: TableRow, y: TableRow): Seq[Delta] =
    diff(Option(x), Option(y), schema.getFields.asScala.toList, "")

  private type Record = java.util.Map[String, AnyRef]

  // TableSchema is not serializable
  private val schemaString: String =
    new ObjectMapper()
      .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
      .writeValueAsString(tableSchema)
  private lazy val schema: TableSchema =
    new JsonObjectParser(new GsonFactory)
      .parseAndClose(new StringReader(schemaString), classOf[TableSchema])

  private def diff(
    x: Option[Record],
    y: Option[Record],
    fields: Seq[TableFieldSchema],
    root: String
  ): Seq[Delta] = {
    def getField(f: String)(x: Record): Option[AnyRef] =
      Option(x.get(f))

    fields
      .flatMap { f =>
        val name = f.getName
        val fullName = if (root.isEmpty) name else root + "." + name
        if (f.getType == "RECORD" && f.getMode != "REPEATED") {
          val a = x.flatMap(r => getField(name)(r).map(_.asInstanceOf[Record]))
          val b = y.flatMap(r => getField(name)(r).map(_.asInstanceOf[Record]))
          if (a.isEmpty && b.isEmpty) {
            Nil
          } else if (a.isEmpty || b.isEmpty) {
            Seq(Delta(fullName, a, b, UnknownDelta))
          } else {
            diff(a, b, f.getFields.asScala.toList, fullName)
          }
        } else if (f.getMode == "REPEATED" && unordered.contains(fullName)) {
          if (
            f.getType == "RECORD"
            && unorderedFieldKeys.contains(fullName)
          ) {
            val l = x
              .flatMap(outer =>
                getField(name)(outer).map(_.asInstanceOf[java.util.List[Record]].asScala.toList)
              )
              .getOrElse(List())
              .flatMap(inner =>
                Try(inner.get(unorderedFieldKeys(fullName))).toOption.map(k => (k, inner))
              )
              .toMap
            val r = y
              .flatMap(outer =>
                getField(name)(outer).map(_.asInstanceOf[java.util.List[Record]].asScala.toList)
              )
              .getOrElse(List())
              .flatMap(inner =>
                Try(inner.get(unorderedFieldKeys(fullName))).toOption.map(k => (k, inner))
              )
              .toMap
            (l.keySet ++ r.keySet).flatMap(k =>
              diff(l.get(k), r.get(k), f.getFields.asScala.toList, fullName)
            )
          } else {
            val a = x
              .flatMap(r => Option(r.get(name).asInstanceOf[java.util.List[AnyRef]]))
              .map(sortList)
            val b = y
              .flatMap(r => Option(r.get(name).asInstanceOf[java.util.List[AnyRef]]))
              .map(sortList)
            if (a == b) Nil else Seq(Delta(fullName, a, b, delta(a.orNull, b.orNull)))
          }
        } else {
          val a = x.flatMap(r => getField(name)(r))
          val b = y.flatMap(r => getField(name)(r))
          if (a == b) Nil else Seq(Delta(fullName, a, b, delta(a.orNull, b.orNull)))
        }
      }
      .filter(d => !ignore.contains(d.field))
  }
}

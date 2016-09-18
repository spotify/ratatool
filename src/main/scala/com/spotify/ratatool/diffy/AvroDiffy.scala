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

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import scala.collection.JavaConverters._

/** Field level diff tool for Avro records. */
class AvroDiffy[T <: GenericRecord](val ignore: Set[String] = Set.empty,
                                    val unordered: Set[String] = Set.empty) extends Diffy[T] {

  override def apply(x: T, y: T): Seq[Delta] = {
    require(x.getSchema == y.getSchema)
    diff(x, y, "")
  }

  // scalastyle:off cyclomatic.complexity
  private def diff(x: GenericRecord, y: GenericRecord, root: String): Seq[Delta] = {
    x.getSchema.getFields.asScala.flatMap { f =>
      val name = f.name()
      val fullName = if (root.isEmpty) name else root + "." + name
      getRawType(f.schema()).getType match {
        case Schema.Type.RECORD =>
          val a = x.get(name).asInstanceOf[GenericRecord]
          val b = y.get(name).asInstanceOf[GenericRecord]
          if (a == null && b == null) {
            Nil
          } else if (a == null || b == null) {
            Seq(Delta(fullName, a, b, UnknownDelta))
          } else {
            diff(a, b, fullName)
          }
        case Schema.Type.ARRAY if unordered.contains(fullName) =>
          val a = DiffyUtils.sortList(x.get(name).asInstanceOf[java.util.List[AnyRef]])
          val b = DiffyUtils.sortList(y.get(name).asInstanceOf[java.util.List[AnyRef]])
          if (a == b) Nil else Seq(Delta(fullName, a, b, delta(a, b)))
        case _ =>
          val a = x.get(name)
          val b = y.get(name)
          if (a == b) Nil else Seq(Delta(fullName, a, b, delta(a, b)))
      }
    }
    .filter(d => !ignore.contains(d.field))
  }
  // scalastyle:on cyclomatic.complexity

  private def getRawType(schema: Schema): Schema = {
    schema.getType match {
      case Schema.Type.UNION =>
        val types = schema.getTypes
        if (types.size == 2) {
          if (types.get(0).getType == Schema.Type.NULL) {
            types.get(1)
          } else if (types.get(1).getType == Schema.Type.NULL) {
            // incorrect use of Avro "nullable" but happens
            types.get(0)
          } else {
            schema
          }
        } else {
          schema
        }
      case _ => schema
    }
  }

}

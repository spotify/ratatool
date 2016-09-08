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
class AvroDiffy[T <: GenericRecord] extends Diffy[T] {

  override def apply(x: T, y: T): Seq[Delta] = {
    require(x.getSchema == y.getSchema)
    diff(x, y, "")
  }

  private def diff(x: GenericRecord, y: GenericRecord, root: String): Seq[Delta] = {
    x.getSchema.getFields.asScala.flatMap { f =>
      val name = f.name()
      val fullName = if (root.isEmpty) name else root + "." + name
      val schema = getRawType(f.schema())
      schema.getType match {
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
        case _ =>
          val a = x.get(name)
          val b = y.get(name)
          if (a == b) Nil else Seq(Delta(fullName, a, b, delta(a, b)))
      }
    }
  }

  private def getRawType(schema: Schema): Schema = {
    schema.getType match {
      case Schema.Type.UNION =>
        assert(schema.getTypes.size == 2 && schema.getTypes.get(0).getType == Schema.Type.NULL)
        schema.getTypes.get(1)
      case _ => schema
    }
  }

}

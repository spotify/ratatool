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

import com.spotify.scio.coders.Coder
import org.apache.avro.{Schema, SchemaValidatorBuilder}
import org.apache.avro.generic.GenericRecord

import scala.collection.JavaConverters._

/** Field level diff tool for Avro records. */
class AvroDiffy[T <: GenericRecord: Coder](ignore: Set[String] = Set.empty,
                                    unordered: Set[String] = Set.empty,
                                    unorderedFieldKeys: Map[String, String] = Map())
  extends Diffy[T](ignore, unordered, unorderedFieldKeys) {

  override def apply(x: T, y: T): Seq[Delta] = {
    new SchemaValidatorBuilder().canReadStrategy.validateLatest()
      .validate(y.getSchema, List(x.getSchema).asJava)
    diff(x, y, "")
  }

  // scalastyle:off cyclomatic.complexity
  private def diff(x: GenericRecord, y: GenericRecord, root: String): Seq[Delta] = {
    def getField(f: String)(x: GenericRecord): AnyRef = {
      x.get(f)
    }

    y.getSchema.getFields.asScala.flatMap { f =>
      val name = f.name()
      val fullName = if (root.isEmpty) name else root + "." + name
      getRawType(f.schema()).getType match {
        case Schema.Type.RECORD =>
          val a = x.get(name).asInstanceOf[GenericRecord]
          val b = y.get(name).asInstanceOf[GenericRecord]
          if (a == null && b == null) {
            Nil
          } else if (a == null || b == null) {
            Seq(Delta(fullName, Option(a), Option(b), UnknownDelta))
          } else {
            diff(a, b, fullName)
          }
        case Schema.Type.ARRAY if unordered.contains(fullName) =>
          if (f.schema().getElementType.getType == Schema.Type.RECORD
              && unordered.exists(_.startsWith(s"$fullName."))
              && unorderedFieldKeys.contains(fullName)) {
            val a = sortList(x.get(name).asInstanceOf[java.util.List[GenericRecord]],
              unorderedFieldKeys.get(fullName).map(getField))
            val b = sortList(y.get(name).asInstanceOf[java.util.List[GenericRecord]],
              unorderedFieldKeys.get(fullName).map(getField))
            a.asScala.zip(b.asScala).flatMap{case (l, r) =>
              diff(l.asInstanceOf[GenericRecord], r.asInstanceOf[GenericRecord], fullName)
            }.toList
          }
          else {
            val a = sortList(x.get(name).asInstanceOf[java.util.List[GenericRecord]])
            val b = sortList(y.get(name).asInstanceOf[java.util.List[GenericRecord]])
            if (a == b) Nil else Seq(Delta(fullName, Option(a), Option(b), delta(a, b)))
          }
        case _ =>
          val a = x.get(name)
          val b = y.get(name)
          if (a == b) Nil else Seq(Delta(fullName, Option(a), Option(b), delta(a, b)))
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

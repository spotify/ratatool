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
import scala.util.Try

import scala.jdk.CollectionConverters._

/** Field level diff tool for Avro records. */
class AvroDiffy[T <: GenericRecord: Coder](
  ignore: Set[String] = Set.empty,
  unordered: Set[String] = Set.empty,
  unorderedFieldKeys: Map[String, String] = Map()
) extends Diffy[T](ignore, unordered, unorderedFieldKeys) {

  override def apply(x: T, y: T): Seq[Delta] = {
    new SchemaValidatorBuilder().canReadStrategy
      .validateLatest()
      .validate(y.getSchema, List(x.getSchema).asJava)
    diff(Option(x), Option(y), "")
  }

  def isAvroRecordType(schema: Schema): Boolean =
    Schema.Type.RECORD.equals(schema.getType) ||
      (Schema.Type.UNION.equals(schema.getType) &&
        schema.getTypes.asScala.map(_.getType).contains(Schema.Type.RECORD))

  private def diff(x: Option[GenericRecord], y: Option[GenericRecord], root: String): Seq[Delta] = {
    // If a y exists we assume it has the superset of all fields, since x must be backwards
    // compatible with it based on the SchemaValidator check in apply()
    val schemaFields = (x, y) match {
      case (Some(xVal), None) => xVal.getSchema.getFields.asScala.toList
      case (_, Some(yVal))    => yVal.getSchema.getFields.asScala.toList
      case _                  => List()
    }

    schemaFields
      .flatMap { f =>
        val name = f.name()
        val fullName = if (root.isEmpty) name else root + "." + name
        getRawType(f.schema()).getType match {
          case Schema.Type.RECORD =>
            val a = x.flatMap(r => Option(r.get(name).asInstanceOf[GenericRecord]))
            val b = y.flatMap(r => Option(r.get(name).asInstanceOf[GenericRecord]))
            (a, b) match {
              case (None, None)       => Nil
              case (Some(_), None)    => Seq(Delta(fullName, a, None, UnknownDelta))
              case (None, Some(_))    => Seq(Delta(fullName, None, b, UnknownDelta))
              case (Some(_), Some(_)) => diff(a, b, fullName)
            }
          case Schema.Type.ARRAY if unordered.contains(fullName) =>
            if (
              unorderedFieldKeys.contains(fullName)
              && isAvroRecordType(f.schema().getElementType)
            ) {
              val l = x
                .flatMap(outer =>
                  Option(outer.get(name).asInstanceOf[java.util.List[GenericRecord]].asScala.toList)
                )
                .getOrElse(List())
                .flatMap(inner =>
                  Try(inner.get(unorderedFieldKeys(fullName))).toOption.map(k => (k, inner))
                )
                .toMap
              val r = y
                .flatMap(outer =>
                  Option(outer.get(name).asInstanceOf[java.util.List[GenericRecord]].asScala.toList)
                )
                .getOrElse(List())
                .flatMap(inner =>
                  Try(inner.get(unorderedFieldKeys(fullName))).toOption.map(k => (k, inner))
                )
                .toMap
              (l.keySet ++ r.keySet).flatMap(k => diff(l.get(k), r.get(k), fullName)).toList
            } else {
              val a = x
                .flatMap(r => Option(r.get(name).asInstanceOf[java.util.List[GenericRecord]]))
                .map(sortList)
              val b = y
                .flatMap(r => Option(r.get(name).asInstanceOf[java.util.List[GenericRecord]]))
                .map(sortList)
              if (a == b) {
                Nil
              } else {
                Seq(Delta(fullName, a, b, delta(a.orNull, b.orNull)))
              }
            }
          case _ =>
            val a = x.flatMap(r => Option(r.get(name)))
            val b = y.flatMap(r => Option(r.get(name)))
            if (a == b) Nil else Seq(Delta(fullName, a, b, delta(a.orNull, b.orNull)))
        }
      }
      .filter(d => !ignore.contains(d.field))
  }

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

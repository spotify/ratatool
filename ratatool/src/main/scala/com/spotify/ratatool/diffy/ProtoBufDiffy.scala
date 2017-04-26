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

import com.google.protobuf.Descriptors.FieldDescriptor.JavaType
import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}
import com.google.protobuf.GeneratedMessage

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/** Field level diff tool for ProtoBuf records. */
class ProtoBufDiffy[T <: GeneratedMessage : ClassTag](ignore: Set[String] = Set.empty,
                                                      unordered: Set[String] = Set.empty)
  extends Diffy[T](ignore, unordered) {

  override def apply(x: T, y: T): Seq[Delta] = diff(x, y, descriptor.getFields.asScala, "")

  // Descriptor is not serializable
  private lazy val descriptor: Descriptor =
    implicitly[ClassTag[T]].runtimeClass
      .getMethod("getDescriptor")
      .invoke(null).asInstanceOf[Descriptor]

  // scalastyle:off cyclomatic.complexity
  private def diff(x: GeneratedMessage, y: GeneratedMessage,
                   fields: Seq[FieldDescriptor], root: String): Seq[Delta] = {
    def getField(m: GeneratedMessage, f: FieldDescriptor): AnyRef =
      if (f.isRepeated) {
        m.getField(f)
      } else {
        if (m.hasField(f)) m.getField(f) else null
      }

    fields.flatMap { f =>
      val name = f.getName
      val fullName = if (root.isEmpty) name else root + "." + name
      if (f.isRepeated && unordered.contains(fullName)) {
        val a = sortList(x.getField(f).asInstanceOf[java.util.List[AnyRef]])
        val b = sortList(y.getField(f).asInstanceOf[java.util.List[AnyRef]])
        if (a == b) Nil else Seq(Delta(fullName, a, b, delta(a, b)))
      } else {
        f.getJavaType match {
          case JavaType.MESSAGE if !f.isRepeated =>
            val a = getField(x, f).asInstanceOf[GeneratedMessage]
            val b = getField(y, f).asInstanceOf[GeneratedMessage]
            if (a == null && b == null) {
              Nil
            } else if (a == null || b == null) {
              Seq(Delta(fullName, a, b, UnknownDelta))
            } else {
              diff(a, b, f.getMessageType.getFields.asScala, fullName)
            }
          case _ =>
            val a = x.getField(f)
            val b = y.getField(f)
            if (a == b) Nil else Seq(Delta(fullName, a, b, delta(a, b)))
        }
      }
    }
    .filter(d => !ignore.contains(d.field))
  }
  // scalastyle:on cyclomatic.complexity

}

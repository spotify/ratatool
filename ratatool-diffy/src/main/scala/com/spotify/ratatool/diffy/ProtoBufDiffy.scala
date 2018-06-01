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
import com.google.protobuf.AbstractMessage

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/** Field level diff tool for ProtoBuf records. */
class ProtoBufDiffy[T <: AbstractMessage : ClassTag](ignore: Set[String] = Set.empty,
                                                     unordered: Set[String] = Set.empty,
                                                     unorderedFieldKeys: Map[String, String]= Map())
extends Diffy[T](ignore, unordered, unorderedFieldKeys) {

  override def apply(x: T, y: T): Seq[Delta] = diff(x, y, descriptor.getFields.asScala, "")

  // Descriptor is not serializable
  private lazy val descriptor: Descriptor =
    implicitly[ClassTag[T]].runtimeClass
      .getMethod("getDescriptor")
      .invoke(null).asInstanceOf[Descriptor]

  // scalastyle:off cyclomatic.complexity method.length
  private def diff(x: AbstractMessage, y: AbstractMessage,
                   fields: Seq[FieldDescriptor], root: String): Seq[Delta] = {
    def getField(f: FieldDescriptor)(m: AbstractMessage): AnyRef =
      if (f.isRepeated) {
        m.getField(f)
      } else {
        if (m.hasField(f)) m.getField(f) else null
      }

    def getFieldDescriptor(f: FieldDescriptor, s: String): FieldDescriptor = {
      f.getMessageType.getFields.asScala.find(field => field.getName == s) match {
        case Some(d) => d
        case None => throw new Exception(s"Field $s not found in ${f.getFullName}")
      }
    }

    fields.flatMap { f =>
      val name = f.getName
      val fullName = if (root.isEmpty) name else root + "." + name
      if (f.isRepeated && unordered.contains(fullName)) {
        val getFieldFn: Option[AbstractMessage => AnyRef] =
          unorderedFieldKeys.get(fullName).map(s => getField(getFieldDescriptor(f, s)) _)
        val a = sortList(x.getField(f).asInstanceOf[java.util.List[AbstractMessage]], getFieldFn)
        val b = sortList(y.getField(f).asInstanceOf[java.util.List[AbstractMessage]], getFieldFn)
        if (f.getJavaType == JavaType.MESSAGE && unordered.exists(_.startsWith(s"$fullName."))
            && unorderedFieldKeys.contains(fullName)) {
          a.asInstanceOf[java.util.List[AbstractMessage]].asScala.zip(
            b.asInstanceOf[java.util.List[AbstractMessage]].asScala).flatMap {
              case (l, r) => diff(l, r, f.getMessageType.getFields.asScala, fullName)}
        }
        else {
          if (a == b) Nil else Seq(Delta(fullName, a, b, delta(a, b)))
        }
      } else {
        f.getJavaType match {
          case JavaType.MESSAGE if !f.isRepeated =>
            val a = getField(f)(x).asInstanceOf[AbstractMessage]
            val b = getField(f)(y).asInstanceOf[AbstractMessage]
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

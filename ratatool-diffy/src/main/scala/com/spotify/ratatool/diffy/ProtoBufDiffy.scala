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

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.Try

/** Field level diff tool for ProtoBuf records. */
class ProtoBufDiffy[T <: AbstractMessage: ClassTag](
  ignore: Set[String] = Set.empty,
  unordered: Set[String] = Set.empty,
  unorderedFieldKeys: Map[String, String] = Map()
) extends Diffy[T](ignore, unordered, unorderedFieldKeys) {

  override def apply(x: T, y: T): Seq[Delta] =
    diff(Option(x), Option(y), descriptor.getFields.asScala.toList, "")

  // Descriptor is not serializable
  private lazy val descriptor: Descriptor =
    implicitly[ClassTag[T]].runtimeClass
      .getMethod("getDescriptor")
      .invoke(null)
      .asInstanceOf[Descriptor]

  private def diff(
    x: Option[AbstractMessage],
    y: Option[AbstractMessage],
    fields: Seq[FieldDescriptor],
    root: String
  ): Seq[Delta] = {
    def getField(f: FieldDescriptor)(m: AbstractMessage): Option[AnyRef] =
      if (f.isRepeated) {
        Option(m.getField(f))
      } else {
        if (m.hasField(f)) Option(m.getField(f)) else None
      }

    def getFieldDescriptor(f: FieldDescriptor, s: String): FieldDescriptor = {
      f.getMessageType.getFields.asScala.find(field => field.getName == s) match {
        case Some(d) => d
        case None    => throw new Exception(s"Field $s not found in ${f.getFullName}")
      }
    }

    fields
      .flatMap { f =>
        val name = f.getName
        val fullName = if (root.isEmpty) name else root + "." + name
        if (f.isRepeated && unordered.contains(fullName)) {
          if (
            f.getJavaType == JavaType.MESSAGE
            && unorderedFieldKeys.contains(fullName)
          ) {
            val l = x
              .flatMap(outer =>
                Option(outer.getField(f).asInstanceOf[java.util.List[AbstractMessage]].asScala)
              )
              .getOrElse(List())
              .flatMap(inner =>
                Try(getFieldDescriptor(f, unorderedFieldKeys(fullName))).toOption
                  .flatMap(fd => getField(fd)(inner))
                  .map(k => (k, inner))
              )
              .toMap
            val r = y
              .flatMap(outer =>
                Option(outer.getField(f).asInstanceOf[java.util.List[AbstractMessage]].asScala)
              )
              .getOrElse(List())
              .flatMap(inner =>
                Try(getFieldDescriptor(f, unorderedFieldKeys(fullName))).toOption
                  .flatMap(fd => getField(fd)(inner))
                  .map(k => (k, inner))
              )
              .toMap
            (l.keySet ++ r.keySet).flatMap(k =>
              diff(l.get(k), r.get(k), f.getMessageType.getFields.asScala.toList, fullName)
            )
          } else {
            val a = x
              .flatMap(r => Option(r.getField(f).asInstanceOf[java.util.List[AbstractMessage]]))
              .map(sortList)
            val b = y
              .flatMap(r => Option(r.getField(f).asInstanceOf[java.util.List[AbstractMessage]]))
              .map(sortList)
            if (a == b) Nil else Seq(Delta(fullName, a, b, delta(a.orNull, b.orNull)))
          }
        } else {
          f.getJavaType match {
            case JavaType.MESSAGE if !f.isRepeated =>
              val a = x.flatMap(m => getField(f)(m).asInstanceOf[Option[AbstractMessage]])
              val b = y.flatMap(m => getField(f)(m).asInstanceOf[Option[AbstractMessage]])
              if (a.isEmpty && b.isEmpty) {
                Nil
              } else if (a.isEmpty || b.isEmpty) {
                Seq(Delta(fullName, a, b, UnknownDelta))
              } else {
                diff(a, b, f.getMessageType.getFields.asScala.toList, fullName)
              }
            case _ =>
              val a = x.flatMap(r => Option(r.getField(f)))
              val b = y.flatMap(r => Option(r.getField(f)))
              if (a == b) Nil else Seq(Delta(fullName, a, b, delta(a.orNull, b.orNull)))
          }
        }
      }
      .filter(d => !ignore.contains(d.field))
  }

}

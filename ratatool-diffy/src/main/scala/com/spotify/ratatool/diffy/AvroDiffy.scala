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
import org.apache.avro.{Conversion, Schema}
import org.apache.avro.generic.{GenericData, GenericRecord, IndexedRecord}
import org.apache.avro.specific.SpecificData

import scala.jdk.CollectionConverters._
import scala.util.Try

/** Field level diff tool for Avro records. */
class AvroDiffy[T <: IndexedRecord: Coder](
  ignore: Set[String] = Set.empty,
  unordered: Set[String] = Set.empty,
  unorderedFieldKeys: Map[String, String] = Map()
) extends Diffy[T](ignore, unordered, unorderedFieldKeys) {

  private lazy val avroRuntimeVersion =
    Option(classOf[Schema].getPackage.getImplementationVersion)

  // after avro 1.8, use SpecificData.getForClass
  private def dataForClass(cls: Class[_]): SpecificData = Try {
    val modelField = cls.getDeclaredField("MODEL$")
    modelField.setAccessible(true)
    val data = modelField.get(null).asInstanceOf[SpecificData]

    // avro 1.8 generated code does not add conversions to the data
    if (avroRuntimeVersion.exists(_.startsWith("1.8."))) {
      val conversionsField = cls.getDeclaredField("conversions")
      conversionsField.setAccessible(true)
      val conversions = conversionsField.get(null).asInstanceOf[Array[Conversion[_]]]
      conversions.filterNot(_ == null).foreach(data.addLogicalTypeConversion)
    }

    data
  }.recover { case _: NoSuchFieldException | _: IllegalAccessException =>
    // Return default instance
    SpecificData.get()
  }.get

  override def apply(x: T, y: T): Seq[Delta] = (x, y) match {
    case (null, null)                    => Seq.empty
    case (_, null)                       => Seq(Delta("", Some(x), None, UnknownDelta))
    case (null, _)                       => Seq(Delta("", None, Some(y), UnknownDelta))
    case _ if x.getSchema != y.getSchema => Seq(Delta("", Some(x), Some(y), UnknownDelta))
    case _                               => diff(x, y, x.getSchema, "")
  }

  private def isRecord(schema: Schema): Boolean = schema.getType match {
    case Schema.Type.RECORD => true
    case Schema.Type.UNION  => schema.getTypes.asScala.map(_.getType).contains(Schema.Type.RECORD)
    case _                  => false
  }

  private def isNumericType(`type`: Schema.Type): Boolean = `type` match {
    case Schema.Type.INT | Schema.Type.LONG | Schema.Type.FLOAT | Schema.Type.DOUBLE => true
    case _                                                                           => false
  }

  private def numericValue(value: AnyRef): Double = value match {
    case i: java.lang.Integer => i.toDouble
    case l: java.lang.Long    => l.toDouble
    case f: java.lang.Float   => f.toDouble
    case d: java.lang.Double  => d
    case _ => throw new IllegalArgumentException(s"Unsupported numeric type: ${value.getClass}")
  }

  private def diff(x: AnyRef, y: AnyRef, schema: Schema, field: String): Seq[Delta] = {
    val deltas = schema.getType match {
      case Schema.Type.UNION =>
        val data = SpecificData.get()
        val xTypeIndex = data.resolveUnion(schema, x)
        val yTypeIndex = data.resolveUnion(schema, y)
        if (xTypeIndex != yTypeIndex) {
          // Use Option as x or y can be null
          Seq(Delta(field, Option(x), Option(y), UnknownDelta))
        } else {
          // same fields, refined schema
          val fieldSchema = schema.getTypes.get(xTypeIndex)
          diff(x, y, fieldSchema, field)
        }
      case Schema.Type.RECORD =>
        val a = x.asInstanceOf[IndexedRecord]
        val b = y.asInstanceOf[IndexedRecord]
        for {
          f <- schema.getFields.asScala.toSeq
          pos = f.pos()
          name = f.name()
          fullName = if (field.isEmpty) name else field + "." + name
          delta <- diff(a.get(pos), b.get(pos), f.schema(), fullName)
        } yield delta
      case Schema.Type.ARRAY
          if unorderedFieldKeys.contains(field) && isRecord(schema.getElementType) =>
        val keyField = unorderedFieldKeys(field)
        val as =
          x.asInstanceOf[java.util.List[GenericRecord]].asScala.map(r => r.get(keyField) -> r).toMap
        val bs =
          y.asInstanceOf[java.util.List[GenericRecord]].asScala.map(r => r.get(keyField) -> r).toMap

        for {
          k <- (as.keySet ++ bs.keySet).toSeq
          elementField = field + s"[$k]"
          delta <- (as.get(k), bs.get(k)) match {
            case (Some(a), Some(b)) => diff(a, b, schema.getElementType, field)
            case (a, b)             => Seq(Delta(field, a, b, UnknownDelta))
          }
        } yield delta.copy(field = delta.field.replaceFirst(field, elementField))
      case Schema.Type.ARRAY =>
        val xs = x.asInstanceOf[java.util.List[AnyRef]]
        val ys = y.asInstanceOf[java.util.List[AnyRef]]
        val (as, bs) = if (unordered.contains(field)) {
          (sortList(xs).asScala, sortList(ys).asScala)
        } else {
          (xs.asScala, ys.asScala)
        }

        val delta = if (as.size != bs.size) {
          Some(UnknownDelta)
        } else if (isNumericType(schema.getElementType.getType) && as != bs) {
          Some(VectorDelta(vectorDelta(as.map(numericValue).toSeq, bs.map(numericValue).toSeq)))
        } else if (as != bs) {
          as.zip(bs)
            .find { case (a, b) =>
              a != b && diff(a, b, schema.getElementType, field).nonEmpty
            }
            .map(_ => UnknownDelta)
        } else {
          None
        }
        delta.map(d => Delta(field, Some(x), Some(y), d)).toSeq
      case Schema.Type.MAP =>
        val as = x.asInstanceOf[java.util.Map[CharSequence, AnyRef]].asScala.map { case (k, v) =>
          k.toString -> v
        }
        val bs = y.asInstanceOf[java.util.Map[CharSequence, AnyRef]].asScala.map { case (k, v) =>
          k.toString -> v
        }

        for {
          k <- (as.keySet ++ bs.keySet).toSeq
          elementField = field + s"[$k]"
          delta <- (as.get(k), bs.get(k)) match {
            case (Some(a), Some(b)) => diff(a, b, schema.getValueType, field)
            case (a, b)             => Seq(Delta(field, a, b, UnknownDelta))
          }
        } yield delta.copy(field = delta.field.replaceFirst(field, elementField))
      case Schema.Type.STRING =>
        val a = x.asInstanceOf[CharSequence].toString
        val b = y.asInstanceOf[CharSequence].toString
        val delta = if (a == b) None else Some(StringDelta(stringDelta(a, b)))
        delta.map(d => Delta(field, Some(x), Some(y), d)).toSeq
      case t if isNumericType(t) =>
        val a = numericValue(x)
        val b = numericValue(y)
        val delta = if (a == b) None else Some(NumericDelta(numericDelta(a, b)))
        delta.map(d => Delta(field, Some(x), Some(y), d)).toSeq
      case _ =>
        val delta = if (x == y) None else Some(UnknownDelta)
        delta.map(d => Delta(field, Some(x), Some(y), d)).toSeq
    }

    deltas.filterNot(d => ignore.contains(d.field))
  }
}

/*
 * Copyright 2018 Spotify AB.
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

package com.spotify.ratatool.scalacheck

import java.nio.ByteBuffer
import java.util
import com.google.api.services.bigquery.model.{TableFieldSchema, TableRow, TableSchema}
import com.google.common.io.BaseEncoding
import com.spotify.ratatool.BigQueryUtil.getFieldModeWithDefault
import org.joda.time._
import org.joda.time.format.DateTimeFormat
import org.scalacheck.{Arbitrary, Gen}

import scala.jdk.CollectionConverters._

/** Mainly type inference not to fall into `Any` */
private class TableFieldValue(val name: String, val value: Any)

private object TableFieldValue {
  def apply(n: String, x: Int): TableFieldValue = new TableFieldValue(n, x)
  def apply(n: String, x: Float): TableFieldValue = new TableFieldValue(n, x)
  def apply(n: String, x: Instant): TableFieldValue = new TableFieldValue(n, x)
  def apply(n: String, x: LocalDateTime): TableFieldValue = new TableFieldValue(n, x)
  def apply(n: String, x: Null): TableFieldValue = new TableFieldValue(n, x)
  def apply(n: String, x: ByteBuffer): TableFieldValue = new TableFieldValue(n, x)
  def apply(n: String, x: Boolean): TableFieldValue = new TableFieldValue(n, x)
  def apply(n: String, x: String): TableFieldValue = new TableFieldValue(n, x)
  def apply(n: String, x: LocalDate): TableFieldValue = new TableFieldValue(n, x)
  def apply(n: String, x: LocalTime): TableFieldValue = new TableFieldValue(n, x)
  def apply(n: String, x: TableRow): TableFieldValue = new TableFieldValue(n, x)
  def apply(n: String, x: List[TableFieldValue]): TableFieldValue =
    new TableFieldValue(n, x)
  def apply(n: String, x: util.LinkedHashMap[String, Any]): TableFieldValue =
    new TableFieldValue(n, x)
}

object TableRowGeneratorOps extends TableRowGeneratorOps

trait TableRowGeneratorOps {
  private val timeStampFormatter =
    DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").withZoneUTC()
  private val dateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd").withZoneUTC()
  private val dateTimeFormatter =
    DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").withZoneUTC()
  private val timeFormatter =
    DateTimeFormat.forPattern("HH:mm:ss.SSSSSS").withZoneUTC()

  private def instantGen: Gen[Instant] = Gen.choose(0L, Long.MaxValue).map(l => new Instant(l))

  /** Generate a BigQuery [[TableRow]] record. */
  def tableRowOf(schema: TableSchema): Gen[TableRow] =
    tableRowOfList(schema.getFields.asScala.toList)

  /**
   * Generate a TableRow from an `Iterable[TableFieldSchema]` so this can be reused by `RECORD`
   * fields
   */
  private def tableRowOfList(row: Iterable[TableFieldSchema]): Gen[TableRow] = {
    def buildRow(r: TableRow, v: List[TableFieldValue]): TableRow = {
      v.foreach { s =>
        s.value match {
          /** Workaround for type erasure */
          case List(_: TableFieldValue, _*) =>
            r.set(s.name, s.value.asInstanceOf[List[TableFieldValue]].map(_.value).asJava)
          case _ => r.set(s.name, s.value)
        }
      }
      r
    }

    val fieldGens = Gen.sequence(row.map(t => tableFieldValueOf(t)))
    fieldGens.map { list =>
      val r = new TableRow
      buildRow(r, list.asScala.toList)
      r
    }
  }

  /**
   * Generate a TableRow from an `Iterable[TableFieldSchema]` so this can be reused by `RECORD`
   * fields
   */
  private def linkedMapOfList(
    row: Iterable[TableFieldSchema]
  ): Gen[util.LinkedHashMap[String, Any]] = {
    def buildMap(
      r: util.LinkedHashMap[String, Any],
      v: List[TableFieldValue]
    ): util.LinkedHashMap[String, Any] = {
      v.foreach { s =>
        s.value match {
          /** Workaround for type erasure */
          case List(_: TableFieldValue, _*) =>
            r.put(s.name, s.value.asInstanceOf[List[TableFieldValue]].map(_.value).asJava)
          case _ => r.put(s.name, s.value)
        }
      }
      r
    }

    val fieldGens = Gen.sequence(row.map(t => tableFieldValueOf(t)))
    fieldGens.map { list =>
      val r = new util.LinkedHashMap[String, Any]()
      buildMap(r, list.asScala.toList)
      r
    }
  }

  private def tableFieldValueOf(fieldSchema: TableFieldSchema): Gen[TableFieldValue] = {
    val n = fieldSchema.getName
    def genV(): Gen[TableFieldValue] = fieldSchema.getType match {
      case "INTEGER" => Arbitrary.arbInt.arbitrary.map(TableFieldValue(n, _))
      case "FLOAT"   => Arbitrary.arbFloat.arbitrary.map(TableFieldValue(n, _))
      case "BOOLEAN" => Arbitrary.arbBool.arbitrary.map(TableFieldValue(n, _))
      case "STRING"  => Arbitrary.arbString.arbitrary.map(TableFieldValue(n, _))
      case "TIMESTAMP" =>
        instantGen.map(i => TableFieldValue(n, timeStampFormatter.print(i) + " UTC"))
      case "DATE"     => instantGen.map(i => TableFieldValue(n, dateFormatter.print(i)))
      case "TIME"     => instantGen.map(i => TableFieldValue(n, timeFormatter.print(i)))
      case "DATETIME" => instantGen.map(i => TableFieldValue(n, dateTimeFormatter.print(i)))
      case "BYTES" =>
        Gen
          .listOf(Arbitrary.arbByte.arbitrary)
          .map(i => ByteBuffer.wrap(i.toArray))
          .map(v => TableFieldValue(n, BaseEncoding.base64().encode(v.array())))
      case "RECORD" =>
        linkedMapOfList(fieldSchema.getFields.asScala).map(TableFieldValue(n, _))

      case t => throw new RuntimeException(s"Unknown type: $t")
    }

    getFieldModeWithDefault(fieldSchema.getMode) match {
      case "REQUIRED" => genV()
      case "NULLABLE" =>
        Arbitrary.arbBool.arbitrary.flatMap { e =>
          if (e) genV() else Gen.const(TableFieldValue(n, null))
        }
      case "REPEATED" => Gen.nonEmptyListOf(genV()).map(l => TableFieldValue(n, l))

      case m => throw new RuntimeException(s"Unknown mode: $m")
    }
  }
}

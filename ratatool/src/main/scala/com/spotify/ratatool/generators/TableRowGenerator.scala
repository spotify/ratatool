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

package com.spotify.ratatool.generators

import java.nio.ByteBuffer
import java.util.Random

import com.google.api.services.bigquery.model.{TableFieldSchema, TableRow, TableSchema}
import com.google.common.io.BaseEncoding
import org.joda.time.Instant
import org.joda.time.format.DateTimeFormat

import scala.collection.JavaConverters._

/** Random generator of BigQuery [[TableRow]] records. */
object TableRowGenerator {

  import Implicits._

  private val random = new Random

  private val timeStampFormatter =
    DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").withZoneUTC()
  private val dateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd").withZoneUTC()
  private val dateTimeFormatter =
    DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").withZoneUTC()
  private val timeFormatter =
    DateTimeFormat.forPattern("HH:mm:ss.SSSSSS").withZoneUTC()

  /** Generate a BigQuery [[TableRow]] record. */
  def tableRowOf(schema: TableSchema): TableRow = randomTableRow(schema, random)

  private def randomTableRow(schema: TableSchema, random: Random): TableRow = {
    val r = new TableRow
    schema.getFields.asScala.foreach { f =>
      r.set(f.getName, generate(f, random, 0))
    }
    r
  }

  private def genInstant: Instant = {
    new Instant(random.nextInt(Int.MaxValue).toLong * 1000 + random.nextInt(1000))
  }

  // scalastyle:off cyclomatic.complexity
  private def generate(schema: TableFieldSchema, random: Random, d: Int): Any = {
    def genV() = schema.getType match {
      case "INTEGER" => random.nextLong()
      case "FLOAT" => random.nextFloat()
      case "BOOLEAN" => random.nextBoolean()
      case "STRING" => random.nextString(40)
      case "TIMESTAMP" => timeStampFormatter.print(genInstant) + " UTC"
      case "DATE" => dateFormatter.print(genInstant)
      case "TIME" => timeFormatter.print(genInstant)
      case "DATETIME" => dateTimeFormatter.print(genInstant)
      case "BYTES" => {
        val bytes = ByteBuffer.allocate(random.nextInt(40)).array()
        random.nextBytes(bytes)
        BaseEncoding.base64().encode(bytes)
      }
      case "RECORD" =>
        val r = new TableRow()
        schema.getFields.asScala.foreach { f =>
          val k = f.getName
          val v = generate(f, random, d + 1)
          r.set(k, v)
        }
        r
      case t => throw new RuntimeException(s"Unknown type: $t")
    }

    schema.getMode match {
      case "REQUIRED" => genV()
      case "NULLABLE" => if (random.nextBoolean()) genV() else null
      case "REPEATED" =>
        val length = random.nextInt(5) + 2 - d
        (1 to math.max(length, 0)).map(_ => genV()).asJava
      case m => throw new RuntimeException(s"Unknown mode: $m")
    }
  }
  // scalastyle:on cyclomatic.complexity

}

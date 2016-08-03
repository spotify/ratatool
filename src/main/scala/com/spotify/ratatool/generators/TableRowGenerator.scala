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

import java.util.Random

import com.google.api.services.bigquery.model.{TableFieldSchema, TableRow, TableSchema}
import org.joda.time.Instant
import org.joda.time.format.DateTimeFormat

import scala.collection.JavaConverters._

object TableRowGenerator {

  import Implicits._

  private val random = new Random

  private val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").withZoneUTC()

  def tableRowOf(schema: TableSchema): TableRow = randomTableRow(schema, random)

  private def randomTableRow(schema: TableSchema, random: Random): TableRow = {
    val r = new TableRow
    schema.getFields.asScala.foreach { f =>
      r.set(f.getName, generate(f, random, 0))
    }
    r
  }

  private def generate(schema: TableFieldSchema, random: Random, d: Int): Any = {
    def genV() = schema.getType match {
      case "INTEGER" => random.nextLong()
      case "FLOAT" => random.nextFloat()
      case "BOOLEAN" => random.nextBoolean()
      case "STRING" => random.nextString(40)
      case "TIMESTAMP" =>
        val i = new Instant(random.nextInt(Int.MaxValue).toLong * 1000 + random.nextInt(1000))
        formatter.print(i) + " UTC"
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

}

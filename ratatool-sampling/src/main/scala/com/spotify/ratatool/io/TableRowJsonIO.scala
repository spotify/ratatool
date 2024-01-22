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

package com.spotify.ratatool.io

import java.io.{File, InputStream, OutputStream}

import com.google.api.services.bigquery.model.TableRow
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Charsets
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.Files
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder
import org.apache.beam.sdk.util.CoderUtils

import scala.jdk.CollectionConverters._
import scala.io.Source

/** Utilities for BigQuery [[TableRow]] JSON IO. */
object TableRowJsonIO {

  private def fromLines(iterator: Iterator[String]): Iterator[TableRow] = {
    val coder = TableRowJsonCoder.of()
    iterator.map(line => CoderUtils.decodeFromByteArray(coder, line.getBytes))
  }

  private def toLines(data: Iterable[TableRow]): Iterable[String] = {
    val coder = TableRowJsonCoder.of()
    data.map(row => new String(CoderUtils.encodeToByteArray(coder, row)))
  }

  /** Read records from a file. */
  def readFromFile(file: File): Iterator[TableRow] = fromLines(Source.fromFile(file).getLines())

  /** Read records from a file. */
  def readFromFile(name: String): Iterator[TableRow] = readFromFile(new File(name))

  /** Read records from an [[InputStream]]. */
  def readFromInputStream(is: InputStream): Iterator[TableRow] =
    fromLines(Source.fromInputStream(is).getLines())

  /** Read records from a resource file. */
  def readFromResource(name: String): Iterator[TableRow] =
    readFromInputStream(this.getClass.getResourceAsStream(name))

  /** Write records to a file. */
  def writeToFile(data: Iterable[TableRow], file: File): Unit =
    Files.asCharSink(file, Charsets.UTF_8).writeLines(toLines(data).asJava)

  /** Write records to a file. */
  def writeToFile(data: Iterable[TableRow], name: String): Unit = writeToFile(data, new File(name))

  /** Write records to an [[OutputStream]]. */
  def writeToOutputStream(data: Iterable[TableRow], os: OutputStream): Unit =
    toLines(data).foreach { line =>
      os.write(line.getBytes)
      os.write(System.lineSeparator().getBytes)
    }

}

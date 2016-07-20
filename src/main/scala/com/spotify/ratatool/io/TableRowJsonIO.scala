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
import com.google.cloud.dataflow.sdk.coders.TableRowJsonCoder
import com.google.cloud.dataflow.sdk.util.CoderUtils
import com.google.common.base.Charsets
import com.google.common.io.Files

import scala.collection.JavaConverters._
import scala.io.Source

object TableRowJsonIO {

  private def fromLines(iterator: Iterator[String]): Iterator[TableRow] = {
    val coder = TableRowJsonCoder.of()
    iterator.map(line => CoderUtils.decodeFromByteArray(coder, line.getBytes))
  }

  private def toLines(data: Iterable[TableRow]): Iterable[String] = {
    val coder = TableRowJsonCoder.of()
    data.map(row => new String(CoderUtils.encodeToByteArray(coder, row)))
  }

  def readFromFile(file: File): Iterator[TableRow] = fromLines(Source.fromFile(file).getLines())

  def readFromFile(name: String): Iterator[TableRow] = readFromFile(new File(name))

  def readFromInputStream(is: InputStream): Iterator[TableRow] =
    fromLines(Source.fromInputStream(is).getLines())

  def readFromResource(name: String): Iterator[TableRow] =
    readFromInputStream(this.getClass.getResourceAsStream(name))

  def writeToFile(data: Iterable[TableRow], file: File): Unit =
    Files.asCharSink(file, Charsets.UTF_8).writeLines(toLines(data).asJava)

  def writeToFile(data: Iterable[TableRow], name :String): Unit = writeToFile(data, new File(name))

  def writeToOutputStream(data: Iterable[TableRow], os: OutputStream): Unit =
    toLines(data).foreach { line =>
      os.write(line.getBytes)
      os.write(System.lineSeparator().getBytes)
    }

}

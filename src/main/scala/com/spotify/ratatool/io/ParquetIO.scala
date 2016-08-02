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
import java.nio.file.Files

import com.spotify.ratatool.GcsConfiguration
import org.apache.avro.generic.GenericRecord
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter}
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetReader, ParquetWriter}

import scala.collection.JavaConverters._

object ParquetIO {

  def readFromFile(path: Path): Iterator[GenericRecord] = {
    val conf = GcsConfiguration.get()
    val fs = FileSystem.get(path.toUri, conf)
    val status = fs.getFileStatus(path)
    val footers = ParquetFileReader.readFooters(conf, status, true).asScala

    footers.map { f =>
      val reader = AvroParquetReader.builder(f.getFile)
        .withConf(conf)
        .build()
        .asInstanceOf[ParquetReader[GenericRecord]]
      new Iterator[GenericRecord] {
        private var item = reader.read()
        override def hasNext: Boolean = item != null
        override def next(): GenericRecord = {
          val r = item
          item = reader.read()
          r
        }
      }
    }.reduce(_++_)
  }

  def readFromFile(name: String): Iterator[GenericRecord] = readFromFile(new Path(name))

  def readFromFile(file: File): Iterator[GenericRecord] = readFromFile(file.getAbsolutePath)

  def readFromInputStream(is: InputStream): Iterator[GenericRecord] = {
    val dir = Files.createTempDirectory("ratatool-")
    val file = new File(dir.toString, "temp.parquet")
    Files.copy(is, file.toPath)
    val data = readFromFile(file)
    FileUtils.deleteDirectory(dir.toFile)
    data
  }

  def readFromResource(name: String): Iterator[GenericRecord] =
    readFromInputStream(this.getClass.getResourceAsStream(name))

  def writeToFile(data: Iterable[GenericRecord], path: Path): Unit = {
    val conf = GcsConfiguration.get()
    val writer = AvroParquetWriter.builder(path)
      .withConf(conf)
      .withSchema(data.head.getSchema)
      .build()
      .asInstanceOf[ParquetWriter[GenericRecord]]
    data.foreach(writer.write)
    writer.close()
  }

  def writeToFile(data: Iterable[GenericRecord], name: String): Unit =
    writeToFile(data, new Path(name))

  def writeToFile(data: Iterable[GenericRecord], file: File): Unit =
    writeToFile(data, file.getAbsolutePath)

  def writeToOutputStream(data: Iterable[GenericRecord], os: OutputStream): Unit = {
    val dir = Files.createTempDirectory("ratatool-")
    val file = new File(dir.toString, "temp.parquet")
    writeToFile(data, file)
    Files.copy(file.toPath, os)
    FileUtils.deleteDirectory(dir.toFile)
  }

}

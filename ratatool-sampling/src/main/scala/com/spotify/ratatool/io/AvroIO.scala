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
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel

import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams
import org.apache.avro.Schema
import org.apache.avro.file.{DataFileReader, DataFileWriter, SeekableByteArrayInput, SeekableInput}
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DatumReader, DatumWriter}
import org.apache.avro.reflect.{ReflectDatumReader, ReflectDatumWriter}
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter, SpecificRecord}
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.io.fs.MatchResult.Metadata

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

/** Utilities for Avro IO. */
object AvroIO {

  private def createDatumReader[T: ClassTag]: DatumReader[T] = {
    val cls = implicitly[ClassTag[T]].runtimeClass
    if (classOf[SpecificRecord] isAssignableFrom cls) {
      new SpecificDatumReader[T]()
    } else if (classOf[GenericRecord] isAssignableFrom cls) {
      new GenericDatumReader[T]()
    } else {
      new ReflectDatumReader[T]()
    }
  }

  private def createDatumWriter[T: ClassTag]: DatumWriter[T] = {
    val cls = implicitly[ClassTag[T]].runtimeClass
    if (classOf[SpecificRecord] isAssignableFrom cls) {
      new SpecificDatumWriter[T]()
    } else if (classOf[GenericRecord] isAssignableFrom cls) {
      new GenericDatumWriter[T]()
    } else {
      new ReflectDatumWriter[T]()
    }
  }

  /** Read records from a file. */
  def readFromFile[T: ClassTag](file: File): Iterator[T] =
    DataFileReader.openReader(file, createDatumReader[T]).iterator().asScala

  /** Read records from a file. */
  def readFromFile[T: ClassTag](name: String): Iterator[T] = readFromFile(new File(name))

  /** Read records from an [[InputStream]]. */
  def readFromInputStream[T: ClassTag](is: InputStream): Iterator[T] = {
    val bytes = ByteStreams.toByteArray(is)
    val input = new SeekableByteArrayInput(bytes)
    DataFileReader.openReader(input, createDatumReader[T]).iterator().asScala
  }

  /** Read records from a resource file. */
  def readFromResource[T: ClassTag](name: String): Iterator[T] =
    readFromInputStream(this.getClass.getResourceAsStream(name))

  /** Write records to a file. */
  def writeToFile[T: ClassTag](data: Iterable[T], schema: Schema, file: File): Unit = {
    val fileWriter = new DataFileWriter(createDatumWriter[T]).create(schema, file)
    data.foreach(fileWriter.append)
    fileWriter.close()
  }

  /** Write records to a file. */
  def writeToFile[T: ClassTag](data: Iterable[T], schema: Schema, name: String): Unit =
    writeToFile(data, schema, new File(name))

  /** Write records to an [[OutputStream]]. */
  def writeToOutputStream[T: ClassTag](
    data: Iterable[T],
    schema: Schema,
    os: OutputStream
  ): Unit = {
    val fileWriter = new DataFileWriter(createDatumWriter[T]).create(schema, os)
    data.foreach(fileWriter.append)
    fileWriter.close()
  }

  def getAvroSchemaFromFile(path: String): Schema = {
    require(FileStorage(path).exists, s"File `$path` does not exist!")
    val files = FileStorage(path).listFiles.filter(_.resourceId.getFilename.endsWith(".avro"))
    require(files.nonEmpty, s"File `$path` does not contain avro files")
    val reader = new GenericDatumReader[GenericRecord]()
    val dfr = new DataFileReader[GenericRecord](AvroIO.getAvroSeekableInput(files.head), reader)
    dfr.getSchema
  }

  private def getAvroSeekableInput(meta: Metadata): SeekableInput = new SeekableInput {
    require(meta.isReadSeekEfficient)
    private val in = FileSystems.open(meta.resourceId()).asInstanceOf[SeekableByteChannel]
    override def read(b: Array[Byte], off: Int, len: Int): Int =
      in.read(ByteBuffer.wrap(b, off, len))
    override def tell(): Long = in.position()
    override def length(): Long = in.size()
    override def seek(p: Long): Unit = in.position(p)
    override def close(): Unit = in.close()
  }

}

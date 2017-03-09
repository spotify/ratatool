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

import com.google.common.io.ByteStreams
import org.apache.avro.Schema
import org.apache.avro.file.{DataFileReader, DataFileWriter, SeekableByteArrayInput}
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DatumReader, DatumWriter}
import org.apache.avro.reflect.{ReflectDatumReader, ReflectDatumWriter}
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter, SpecificRecord}

import scala.collection.JavaConverters._
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
  def writeToOutputStream[T: ClassTag](data: Iterable[T],
                                       schema: Schema,
                                       os: OutputStream): Unit = {
    val fileWriter = new DataFileWriter(createDatumWriter[T]).create(schema, os)
    data.foreach(fileWriter.append)
    fileWriter.close()
  }

  def getAvroSchemaFromFile(path: String): Schema = {
    require(FileStorage(path).exists, s"File `$path` does not exist!")
    val reader = new GenericDatumReader[GenericRecord]()
    val dfr = new DataFileReader[GenericRecord](FileStorage(path).getAvroSeekableInput, reader)
    dfr.getSchema
  }

}

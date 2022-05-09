/*
 * Copyright 2022 Spotify AB.
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

import com.spotify.ratatool.samplers.util.GcsConnectorUtil
import com.spotify.scio.parquet.{BeamInputFile, BeamOutputFile}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.io.fs.ResourceId
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.avro._
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.io.{InputFile, OutputFile}

import java.io.{File, OutputStream}
import java.nio.channels.Channels
import scala.jdk.CollectionConverters._

/** Utilities for Parquet IO. */
object ParquetIO {

  private[ratatool] def getAvroSchemaFromFile(glob: String): Schema = {
    val file = if (!FileSystems.hasGlobWildcard(glob)) {
      glob
    } else {
      // Sample first file match for wildcard pattern
      FileSystems.`match`(glob).metadata().asScala.head.resourceId().toString
    }

    val conf = new Configuration()

    val parquetFileMetadata = ParquetFileReader.open(
      HadoopInputFile.fromPath(new Path(file), conf))
      .getFileMetaData

    // We know Parquet files will either have the parquet.avro.schema key, or have a raw Parquet
    // schema that can be converted to an Avro one.
    Option(parquetFileMetadata.getKeyValueMetaData.get("parquet.avro.schema"))
        .map(new Schema.Parser().parse(_))
        .getOrElse(new AvroSchemaConverter(conf).convert(parquetFileMetadata.getSchema))
  }

  private[ratatool] def genericRecordConfig(schema: Schema): Configuration = {
    val job = Job.getInstance(new Configuration())

    job.getConfiguration.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, true)
    job.getConfiguration.setClass(
      AvroReadSupport.AVRO_DATA_SUPPLIER, classOf[GenericDataSupplier], classOf[AvroDataSupplier]
    )

    AvroParquetInputFormat.setAvroReadSchema(job, schema)
    GcsConnectorUtil.setCredentials(job)
    job.getConfiguration
  }

  /** Read Parquet records from a file into GenericRecords. */
  private[ratatool] def readFromFile(path: InputFile, schema: Schema): Iterator[GenericRecord] = {
    val conf = genericRecordConfig(schema)
    val reader = AvroParquetReader
      .builder[GenericRecord](path)
      .withConf(conf)
      .build()

    new Iterator[GenericRecord] {
      private var item = reader.read()
      override def hasNext: Boolean = item != null
      override def next(): GenericRecord = {
        val r = item
        item = reader.read()
        r
      }
    }
  }

  /** Read Parquet records from a file into GenericRecords. */
  def readFromFile(path: String): Iterator[GenericRecord] =
    readFromFile(BeamInputFile.of(path), getAvroSchemaFromFile(path))

  /** Read Parquet records from a file into GenericRecords. */
  def readFromFile(file: File): Iterator[GenericRecord] = readFromFile(file.getAbsolutePath)

  /** Write records to a file. */
  def writeToFile(data: Iterable[GenericRecord], schema: Schema, output: OutputFile): Unit = {
    val conf = genericRecordConfig(schema)
    val writer = AvroParquetWriter
      .builder[GenericRecord](output)
      .withConf(conf)
      .withSchema(schema)
      .build()
    data.foreach(writer.write)
    writer.close()
  }

  /** Write records to a file. */
  def writeToFile(data: Iterable[GenericRecord], schema: Schema, name: String): Unit =
    writeToFile(data, schema, BeamOutputFile.of(name))

  /** Write records to a file. */
  def writeToFile(data: Iterable[GenericRecord], schema: Schema, file: File): Unit =
    writeToFile(data, schema, file.getAbsolutePath)

  /** Write records to an [[OutputStream]]. */
  def writeToOutputStream(data: Iterable[GenericRecord], schema: Schema, os: OutputStream): Unit = {
    writeToFile(data, schema, BeamOutputFile.of(Channels.newChannel(os)))
  }
}

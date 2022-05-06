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
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.io.fs.ResourceId
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.avro._
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile

import java.io.File

/** Utilities for Parquet IO. */
object ParquetIO {

  private[ratatool] def getAvroSchemaFromFile(file: String): Schema = {
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

  private[ratatool] def avroReadConfig(schema: Schema): Configuration = {
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
  def readFromFile(path: Path): Iterator[GenericRecord] = {
    val schema = getAvroSchemaFromFile(path.toString)
    val conf = avroReadConfig(schema)
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
  def readFromFile(path: String): Iterator[GenericRecord] = readFromFile(new Path(path))

  /** Read Parquet records from a file into GenericRecords. */
  def readFromFile(file: File): Iterator[GenericRecord] = readFromFile(file.getAbsolutePath)
}

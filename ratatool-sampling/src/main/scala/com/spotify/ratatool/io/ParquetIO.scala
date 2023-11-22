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

import com.spotify.ratatool.samplers.util.ParquetGcsConnectorUtil
import com.spotify.scio.parquet.{BeamInputFile, BeamOutputFile}
import org.apache.avro.{Schema, SchemaCompatibility}
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.io.FileSystems
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.avro._
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.io.OutputFile

import java.io.File
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

    val parquetFileMetadata = ParquetFileReader.open(BeamInputFile.of(file)).getFileMetaData

    // We know Parquet files will either have the parquet.avro.schema key, or have a raw Parquet
    // schema that can be converted to an Avro one.
    Option(parquetFileMetadata.getKeyValueMetaData.get("parquet.avro.schema"))
      .map(new Schema.Parser().parse(_))
      .getOrElse(new AvroSchemaConverter(conf).convert(parquetFileMetadata.getSchema))
  }

  private[ratatool] def getCompatibleSchemaForFiles(path1: String, path2: String): Schema = {
    val schemaLhs = getAvroSchemaFromFile(path1)
    val schemaRhs = getAvroSchemaFromFile(path2)
    def isReadCompatible(s1: Schema, s2: Schema): Boolean =
      SchemaCompatibility
        .checkReaderWriterCompatibility(s1, s2)
        .getType == SchemaCompatibilityType.COMPATIBLE

    if (isReadCompatible(schemaLhs, schemaRhs)) {
      schemaLhs
    } else if (isReadCompatible(schemaRhs, schemaLhs)) {
      schemaRhs
    } else {
      throw new IllegalStateException(
        s"input $path1 had an incompatible schema to input " +
          s"$path2: $schemaLhs not compatible with $schemaRhs"
      )
    }
  }

  private[ratatool] def genericRecordReadConfig(schema: Schema, path: String): Configuration = {
    val job = Job.getInstance(new Configuration())

    job.getConfiguration.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, true)
    job.getConfiguration.setClass(
      AvroReadSupport.AVRO_DATA_SUPPLIER,
      classOf[GenericDataSupplier],
      classOf[AvroDataSupplier]
    )

    AvroParquetInputFormat.setAvroReadSchema(job, schema)
    ParquetGcsConnectorUtil.setInputPaths(job, path)
    job.getConfiguration
  }

  /** Read Parquet records from a file into GenericRecords. */
  private[ratatool] def readFromFile(path: String): Iterator[GenericRecord] = {
    val conf = genericRecordReadConfig(getAvroSchemaFromFile(path), path)

    val reader = AvroParquetReader
      .builder[GenericRecord](BeamInputFile.of(path))
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
  def readFromFile(file: File): Iterator[GenericRecord] = readFromFile(file.getAbsolutePath)

  /** Write records to a file. */
  def writeToFile(data: Iterable[GenericRecord], schema: Schema, output: OutputFile): Unit = {
    val writer = AvroParquetWriter
      .builder[GenericRecord](output)
      .withConf(new Configuration())
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
}

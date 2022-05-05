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

import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroSchemaConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile

/** Utilities for Parquet IO. */
object ParquetIO {

  def getAvroSchemaFromFile(file: String): Schema = {
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
}

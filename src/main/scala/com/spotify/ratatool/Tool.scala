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

package com.spotify.ratatool

import java.io.OutputStream

import com.spotify.ratatool.io.{AvroIO, BigQueryIO, ParquetIO, TableRowJsonIO}
import com.spotify.ratatool.samplers.{AvroSampler, BigQuerySampler, ParquetSampler}
import org.apache.hadoop.fs.{FileSystem, Path}

object Tool {

  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      ToolParser.parser.showUsage()
      sys.exit(0)
    }
    val opts = ToolParser.parse(args)
    if (opts.isEmpty) {
      sys.exit(-1)
    }

    val o = opts.get
    o.inMode match {
      case "avro" =>
        val data = new AvroSampler(new Path(o.in)).sample(o.n, o.head)
        AvroIO.writeToFile(data, data.head.getSchema, o.out)
      case "bigquery" =>
        val sampler = new BigQuerySampler(BigQueryIO.parseTableSpec(o.in))
        val data = sampler.sample(o.n, o.head)
        if (o.out.nonEmpty) {
          TableRowJsonIO.writeToFile(data, o.out)
        }
        if (o.tableOut.nonEmpty) {
          val table = BigQueryIO.parseTableSpec(o.tableOut)
          BigQueryIO.writeToTable(data, sampler.schema, table)
        }
      case "parquet" =>
        val data = new ParquetSampler(new Path(o.in)).sample(o.n, o.head)
        ParquetIO.writeToFile(data, data.head.getSchema, o.out)
      case _ =>
        throw new NotImplementedError(s"${o.inMode} not implemented")
    }
  }

  def createOutputStream(name: String): OutputStream = {
    val path = new Path(name)
    val fs = FileSystem.get(path.toUri, GcsConfiguration.get())
    fs.create(path, false)
  }

}

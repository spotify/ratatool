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

package com.spotify.ratatool.tool

import com.spotify.ratatool.Command
import com.spotify.ratatool.diffy.BigDiffy
import com.spotify.ratatool.io.{AvroIO, BigQueryIO, ParquetIO, TableRowJsonIO}
import com.spotify.ratatool.samplers.{AvroSampler, BigQuerySampler, BigSampler, ParquetSampler}

object Ratatool {
  private def commandSet[T <: Command](xs: T*): Set[String] = xs.map(_.command).toSet
  private val commands = commandSet(DirectSamplerParser, BigDiffy, BigSampler)

  def main(args: Array[String]): Unit = {
    val usage = """
      | Ratatool - a tool for random data generation, sampling, and diff-ing
      | Usage: ratatool [bigDiffy|bigSampler|directSampler] [args]
    """.stripMargin

    if (args.isEmpty || !commands.contains(args.head)) {
      print(usage)
      sys.exit(1)
    } else {
      args.head match {
        case BigDiffy.command   => BigDiffy.run(args.tail)
        case BigSampler.command => BigSampler.run(args.tail)
        case DirectSamplerParser.command =>
          val opts = DirectSamplerParser.parse(args.tail)
          if (opts.isEmpty) {
            sys.exit(-1)
          }

          val o = opts.get
          o.mode match {
            case "avro" =>
              val data = new AvroSampler(o.in).sample(o.n, o.head)
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
              val data = new ParquetSampler(o.in).sample(o.n, o.head)
              ParquetIO.writeToFile(data, data.head.getSchema, o.out)
            case _ =>
              throw new NotImplementedError(s"${o.mode} not implemented")
          }
      }
    }
  }
}

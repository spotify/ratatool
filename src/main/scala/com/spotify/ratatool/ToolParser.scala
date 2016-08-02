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

case class ToolConfig(inMode: String = "",
                      in: String = "",
                      out: String = "",
                      tableOut: String = "",
                      n: Long = 1,
                      head: Boolean = false)

object ToolParser {

  // scalastyle:off if.brace
  val parser = new scopt.OptionParser[ToolConfig]("ratatool") {
    head("ratatool - a tool for random data sampling and generation")

    cmd("avro")
      .action((_, c) => c.copy(inMode = "avro"))
      .text("Sample from Avro")
      .children(
        opt[String]("in")
          .required()
          .action((x, c) => c.copy(in = x))
          .text("Avro input path"),
        opt[String]("out")
          .required()
          .action((x, c) => c.copy(out = x))
          .text("Avro output file"))

    note("")  // empty line

    cmd("bigquery")
      .action((_, c) => c.copy(inMode = "bigquery"))
      .text("Sample from BigQuery")
      .children(
        opt[String]("in").required()
          .action((x, c) => c.copy(in = x))
          .text("BigQuery input table"),
        opt[String]("out")
          .action((x, c) => c.copy(out = x))
          .text("TableRow JSON output file"),
        opt[String]("tableOut")
          .action((x, c) => c.copy(tableOut = x))
          .text("BigQuery output table"),
        checkConfig( c =>
          if (c.inMode == "bigquery") {
            if (c.out.isEmpty && c.tableOut.isEmpty)
              failure("Missing output option")
            else if (!c.head)
              failure("BigQuery can only be used in head mode")
            else
              success
          } else {
            success
          }
        ))

    note("")  // empty line

    cmd("parquet")
      .action((_, c) => c.copy(inMode = "parquet"))
      .text("Sample from Parquet")
      .children(
        opt[String]("in")
          .required()
          .action((x, c) => c.copy(in = x))
          .text("Parquet input path"),
        opt[String]("out")
          .required()
          .action((x, c) => c.copy(out = x))
          .text("Parquet output file"),
        checkConfig( c =>
          if (c.inMode == "bigquery") {
            if (!c.head)
              failure("Parquet can only be used in head mode")
            else
              success
          } else {
            success
          }
        ))

    note("")  // empty line
    note("Common options")
    opt[Long]('n', "numSamples")
      .action((x, c) => c.copy(n = x))
      .text("number of samples to collect")
      .required()

    opt[Unit]("head")
      .action((_, c) => c.copy(head = true))
      .text("read from head instead of random sample")

    checkConfig( c =>
      if (c.n <= 0) failure("n must be > 0")
      else if (c.inMode.isEmpty) failure("Missing command")
      else success)
  }
  // scalastyle:on if.brace

  def parse(args: Array[String]): Option[ToolConfig] = parser.parse(args, ToolConfig())

}

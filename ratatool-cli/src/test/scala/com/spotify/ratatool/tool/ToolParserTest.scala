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

import org.scalatest.{FlatSpec, Matchers}

class ToolParserTest extends FlatSpec with Matchers {

  private def parse(cmd: String) = ToolParser.parse(cmd.split(" "))
  private val config = ToolConfig(in = "in", out = "out", n = 1000)

  "ToolParser" should "parse avro command" in {
    val c = config.copy(mode = "avro")
    parse("avro --in in --out out -n 1000") should equal (Some(c))
    parse("avro --in in --out out -n 1000 --head") should equal (Some(c.copy(head = true)))
  }

  it should "parse bigquery command" in {
    val c = config.copy(mode = "bigquery")
    parse("bigquery --in in --out out -n 1000") should equal (None)
    parse("bigquery --in in --out out -n 1000 --head") should equal (Some(c.copy(head = true)))
    parse("bigquery --in in --out out -n 1000 --head --tableOut table:out ") should equal (
      Some(c.copy(head = true, tableOut = "table:out")))
  }

  it should "parse parquet command" in {
    val c = config.copy(mode = "parquet")
    parse("parquet --in in --out out -n 1000") should equal (None)
    parse("parquet --in in --out out -n 1000 --head") should equal (Some(c.copy(head = true)))
  }

}

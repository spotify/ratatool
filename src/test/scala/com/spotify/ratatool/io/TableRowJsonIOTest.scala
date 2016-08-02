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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File}

import com.spotify.ratatool.Schemas
import com.spotify.ratatool.generators.TableRowGenerator
import com.spotify.ratatool.scalacheck.TableRowGen
import org.scalatest.{FlatSpec, Matchers}

class TableRowJsonIOTest extends FlatSpec with Matchers {

  val schema = Schemas.tableSchema
  val gen = TableRowGen.tableRowOf(schema)
  val data = (1 to 100).flatMap(_ => gen.sample)

  "TableRowJsonIO" should "work with stream" in {
    val out = new ByteArrayOutputStream()
    TableRowJsonIO.writeToOutputStream(data, out)
    val in = new ByteArrayInputStream(out.toByteArray)
    val result = TableRowJsonIO.readFromInputStream(in).toList.map(_.toString)
    result should equal (data.map(_.toString))
  }

  it should "work with file" in {
    val file = File.createTempFile("ratatool-", ".json")
    file.deleteOnExit()
    TableRowJsonIO.writeToFile(data, file)
    val result = TableRowJsonIO.readFromFile(file).toList.map(_.toString)
    result should equal (data.map(_.toString))
  }

}

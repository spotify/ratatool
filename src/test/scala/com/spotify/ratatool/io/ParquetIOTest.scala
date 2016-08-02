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
import java.nio.file.Files

import com.spotify.ratatool.Schemas
import com.spotify.ratatool.scalacheck.AvroGen
import org.apache.commons.io.FileUtils
import org.scalatest.{FlatSpec, Matchers}

class ParquetIOTest extends FlatSpec with Matchers {

  val schema = Schemas.avroSchema
  val gen = AvroGen.avroOf(schema)
  val data = (1 to 100).flatMap(_ => gen.sample)

  "ParquetIO" should "work with stream" in {
    val out = new ByteArrayOutputStream()
    ParquetIO.writeToOutputStream(data, schema, out)
    val in = new ByteArrayInputStream(out.toByteArray)
    val result = ParquetIO.readFromInputStream(in).toList
    result should equal (data)
  }

  it should "work with file" in {
    val dir = Files.createTempDirectory("ratatool-")
    val file = new File(dir.toString, "temp.parquet")
    ParquetIO.writeToFile(data, schema, file)
    val result = ParquetIO.readFromFile(file).toList
    result should equal (data)
    FileUtils.deleteDirectory(dir.toFile)
  }

}

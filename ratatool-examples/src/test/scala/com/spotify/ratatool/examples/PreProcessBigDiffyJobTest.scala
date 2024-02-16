/*
 * Copyright 2018 Spotify AB.
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

package com.spotify.ratatool.examples

import com.spotify.ratatool.avro.specific.ExampleRecord
import com.spotify.ratatool.examples.diffy.PreProcessBigDiffy
import com.spotify.ratatool.examples.scalacheck.ExampleAvroGen
import com.spotify.scio.testing._
import org.apache.beam.sdk.util.CoderUtils
import org.scalacheck.Gen
import com.spotify.scio.avro._
import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.io.TextIO

class PreProcessBigDiffyJobTest extends PipelineSpec {

  val coder = CoderMaterializer.beamWithDefault(Coder[ExampleRecord])

  val lhs = Gen.listOfN(1000, ExampleAvroGen.exampleRecordGen).sample.get.map { r =>
    r.setNullableIntField(null)
    r
  }

  val rhs = lhs.map(CoderUtils.clone(coder, _)).map { r =>
    r.setNullableIntField(0)
    r
  }

  val expectedKeys = lhs.map(r => s"${r.getRecordId.toString}\tSAME\t")
  val expectedFieldsCounts = List[String]()
  val expectedTotals = List("1000\t1000\t0\t0\t0")

  "PreProcessBigDiffy Example" should "map nullable int to 0" in {
    JobTest[PreProcessBigDiffy.type]
      .args(
        "--lhs=lhs.avro",
        "--rhs=rhs.avro",
        "--output=output.txt"
      )
      .input(AvroIO[ExampleRecord]("lhs.avro"), lhs)
      .input(AvroIO[ExampleRecord]("rhs.avro"), rhs)
      .output(TextIO("output.txt/keys"))(_ should containInAnyOrder(expectedKeys))
      .output(TextIO("output.txt/fields"))(_ should containInAnyOrder(expectedFieldsCounts))
      .output(TextIO("output.txt/global"))(_ should containInAnyOrder(expectedTotals))
      .run()
  }
}

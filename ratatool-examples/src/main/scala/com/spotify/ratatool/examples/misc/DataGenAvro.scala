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

package com.spotify.ratatool.examples.misc

import com.spotify.ratatool.avro.specific.TestRecord
import com.spotify.scio.ContextAndArgs
import com.spotify.ratatool.scalacheck._
import org.scalacheck.Gen
import com.spotify.scio.avro._

object DataGenAvro {
  def main(args: Array[String]): Unit = {
    val (sc, opts) = ContextAndArgs(args)
    sc.parallelize(1 to 1000)
      .flatMap(_ =>
        Gen
          .listOfN(
            1000,
            avroOf[TestRecord]
              .amend(
                Gen.frequency(
                  (100, "US"),
                  (50, "SE"),
                  (25, "CA"),
                  (50, "UK"),
                  (5, "AU"),
                  (10, "BR")
                )
              )((r: TestRecord) => (s: String) => r.getRequiredFields.setStringField(s))
              .amend(Gen.oneOf((1 to 50000).map(_.toLong)))(_.getRequiredFields.setLongField)
          )
          .sample
          .get
      )
      .saveAsAvroFile(opts("output"), 0)
    sc.run()
  }
}

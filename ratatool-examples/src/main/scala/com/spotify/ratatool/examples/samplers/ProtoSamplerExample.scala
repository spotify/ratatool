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

package com.spotify.ratatool.examples.samplers

import com.spotify.ratatool.proto.Schemas.TestRecord
import com.spotify.scio._
import com.spotify.ratatool.samplers._
import com.spotify.ratatool.samplers.util.StratifiedDistribution


object ProtoSamplerExample {

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    sampleProto[TestRecord](sc.protobufFile[TestRecord](args("input")),
      0.01,
      distribution = Some(StratifiedDistribution),
      distributionFields = Seq("required_fields.string_field")
    ).saveAsProtobufFile(args("output"))


    sc.close().waitUntilDone()
  }
}

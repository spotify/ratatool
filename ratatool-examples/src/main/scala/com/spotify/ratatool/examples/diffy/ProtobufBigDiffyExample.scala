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

package com.spotify.ratatool.examples.diffy

import com.spotify.ratatool.diffy.{BigDiffy, MultiKey, ProtoBufDiffy}
import com.spotify.ratatool.examples.proto.Schemas.ExampleRecord
import com.spotify.scio._

object ProtobufBigDiffyExample {
  def recordKeyFn(t: ExampleRecord): MultiKey =
    MultiKey(t.getStringField)

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val (lhs, rhs, output, header, ignore, unordered) =
      (
        args("lhs"),
        args("rhs"),
        args("output"),
        args.boolean("with-header", false),
        args.list("ignore").toSet,
        args.list("unordered").toSet
      )

    val diffy = new ProtoBufDiffy[ExampleRecord](ignore, unordered)
    val result = BigDiffy.diffProtoBuf[ExampleRecord](sc, lhs, rhs, recordKeyFn, diffy)

    BigDiffy.saveStats(result, output, header)

    sc.run().waitUntilDone()
  }
}

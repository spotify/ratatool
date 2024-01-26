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

import com.spotify.ratatool.avro.specific.ExampleRecord
import com.spotify.ratatool.diffy.{AvroDiffy, BigDiffy, MultiKey}
import com.spotify.scio.ContextAndArgs
import org.apache.beam.sdk.util.CoderUtils
import com.spotify.scio.avro._
import com.spotify.scio.coders.{Coder, CoderMaterializer}

object PreProcessBigDiffy {
  def recordKeyFn(r: ExampleRecord): MultiKey =
    MultiKey(r.getRecordId.toString)

  def mapFn(coder: Coder[ExampleRecord]): ExampleRecord => ExampleRecord = {
    val bCoder = CoderMaterializer.beamWithDefault(coder)

    { (r: ExampleRecord) =>
      val o = CoderUtils.clone(bCoder, r)
      if (o.getNullableIntField == null) {
        o.setNullableIntField(0)
      }
      o
    }
  }

  def main(cmdlineArgs: Array[String]): Unit = {
    val coder = Coder[ExampleRecord]
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val (lhsPath, rhsPath, output, header, ignore, unordered) =
      (
        args("lhs"),
        args("rhs"),
        args("output"),
        args.boolean("with-header", false),
        args.list("ignore").toSet,
        args.list("unordered").toSet
      )

    val lhs = sc.avroFile[ExampleRecord](lhsPath).map(mapFn(coder))
    val rhs = sc.avroFile[ExampleRecord](rhsPath).map(mapFn(coder))
    val diffy = new AvroDiffy[ExampleRecord](ignore, unordered)
    val result = BigDiffy.diff[ExampleRecord](lhs, rhs, diffy, recordKeyFn)

    BigDiffy.saveStats(result, output, header)

    sc.run().waitUntilDone()
  }
}

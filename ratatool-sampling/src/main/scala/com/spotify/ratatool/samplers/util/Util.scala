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

package com.spotify.ratatool.samplers.util

trait SampleDistribution
case object StratifiedDistribution extends SampleDistribution
case object UniformDistribution extends SampleDistribution

object SampleDistribution {
  def fromString(s: String): SampleDistribution = {
    if (s == "stratified") {
      StratifiedDistribution
    }
    else if (s == "uniform") {
      UniformDistribution
    }
    else {
      throw new IllegalArgumentException(s"Invalid distribution $s")
    }
  }
}

trait Determinism
case object NonDeterministic extends Determinism
case object Deterministic extends Determinism

object Determinism {
  def fromSeq(l: Seq[_]): Determinism = {
    if (l == Seq()) {
      NonDeterministic
    } else {
      Deterministic
    }
  }
}

trait Precision
case object Approximate extends Precision
case object Exact extends Precision

object Precision {
  def fromBoolean(exact: Boolean): Precision = {
    if (exact) {
      Exact
    } else {
      Approximate
    }
  }
}

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

package com.spotify.scio

import com.spotify.scio.util.random.RandomSampler

import java.util.concurrent.ConcurrentHashMap

import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import java.util.{Random => JRandom}

object Random {

  /**
   * Modified version of Scio RandomValueSampler to allow sampling on a per-element probability
   */
  abstract class RandomValueSampler[K, V, R]()
    extends DoFn[((K, V), Double), (K, V)] {

    protected lazy val rngs: ConcurrentHashMap[K, R] = new ConcurrentHashMap[K, R]()
    protected var seed: Long = -1

    @ProcessElement
    def processElement(c: DoFn[((K, V), Double), (K, V)]#ProcessContext): Unit = {
      val ((key, value), fraction) = c.element()

      /** Epsilon slop to avoid failure from floating point jitter */
      require(
        fraction >= (0.0 - RandomSampler.roundingEpsilon)
          && fraction <= (1.0 + RandomSampler.roundingEpsilon),
        s"Sampling fractions must be on interval [0, 1], Key: $key, Fraction: $fraction")

      rngs.putIfAbsent(key, init())
      val count = samples(fraction, rngs.get(key))
      var i = 0
      while (i < count) {
        c.output((key, value))
        i += 1
      }
    }

    def init(): R
    def samples(fraction: Double, rng: R): Int
    def setSeed(seed: Long): Unit = this.seed = seed

  }

  class BernoulliValueSampler[K, V]
    extends RandomValueSampler[K, V, JRandom] {

    // TODO: Is seed properly handled here
    // TODO: is it necessary to setSeed for each instance like Spark does?
    override def init(): JRandom = {
      val r = RandomSampler.newDefaultRNG
      if (seed > 0) {
        r.setSeed(seed)
      }
      r
    }

    override def samples(fraction: Double, rng: JRandom): Int =
      if (fraction <= 0.0) {
        0
      } else if (fraction >= 1.0) {
        1
      } else {
        if (rng.nextDouble() <= fraction) 1 else 0
      }
  }

  class RandomValueAssigner[K, V]()
    extends DoFn[(K, V), (K, (V, Double))] {
    protected lazy val rngs: ConcurrentHashMap[K, JRandom] = new ConcurrentHashMap[K, JRandom]()
    protected var seed: Long = -1

    // TODO: is it necessary to setSeed for each instance like Spark does?
    def init(): JRandom = {
      val r = RandomSampler.newDefaultRNG
      if (seed > 0) {
        r.setSeed(seed)
      }
      r
    }

    @ProcessElement
    def processElement(c: DoFn[(K, V), (K, (V, Double))]#ProcessContext): Unit = {
      val (key, value) = c.element()
      rngs.putIfAbsent(key, init())
      c.output((key, (value, rngs.get(key).nextDouble())))
    }

    def setSeed(seed: Long): Unit = this.seed = seed
  }
}

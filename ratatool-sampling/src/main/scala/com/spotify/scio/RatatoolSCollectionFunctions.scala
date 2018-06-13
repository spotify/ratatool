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

import java.util.concurrent.ConcurrentHashMap
import java.util.{Random => JRandom}

import com.spotify.scio.util.random.RandomSampler
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.{ProcessElement, StartBundle}
import org.apache.commons.math3.distribution.{IntegerDistribution, PoissonDistribution}

import scala.reflect.ClassTag

object RatatoolSCollectionFunctions {
  implicit class RatatoolSCollection[T: ClassTag](s: SCollection[T]) {
    def stratifiedSample[U: ClassTag](strataFn: T => U,
                            withReplacement: Boolean,
                            fraction: Double): SCollection[T] = {
      val sampleFn: RandomValueSampler[U, T, _] = if (withReplacement) {
        new PoissonValueSampler[U, T]()
      } else {
        new BernoulliValueSampler[U, T]()
      }
      val keyed = s.keyBy(strataFn)
      val keyCounts = keyed.countByKey
      val tot = keyCounts.values.sum.asSingletonSideInput
      val keyRatios = keyCounts.withSideInputs(tot)
        .map { case ((k, c), sic) => (k, (c.toDouble / sic(tot)) * fraction) }
        .toSCollection
      keyed.join(keyRatios).parDo(sampleFn).values
    }
  }
}


private[scio] abstract class RandomValueSampler[K, V, R]()
  extends DoFn[(K, (V, Double)), (K, V)] {

  protected lazy val rngs: ConcurrentHashMap[K, R] = new ConcurrentHashMap[K, R]()
  protected var seed: Long = -1

//  // TODO: is it necessary to setSeed for each instance like Spark does?
//  @StartBundle
//  def startBundle(c: DoFn[(K, V), (K, V)]#StartBundleContext): Unit =
//    rngs = fractions.mapValues(init).map(identity)  // workaround for serialization issue

  @ProcessElement
  def processElement(c: DoFn[(K, (V, Double)), (K, V)]#ProcessContext): Unit = {
    val (key, (value, fraction)) = c.element()

    rngs.putIfAbsent(key, init(fraction))
    val count = samples(fraction, rngs.get(key))
    var i = 0
    while (i < count) {
      c.output((key, value))
      i += 1
    }
  }

  def init(fraction: Double): R
  def samples(fraction: Double, rng: R): Int
  def setSeed(seed: Long): Unit = this.seed = seed

}

private[scio] class BernoulliValueSampler[K, V]
  extends RandomValueSampler[K, V, JRandom] {

  // TODO: is it necessary to setSeed for each instance like Spark does?
  override def init(fraction: Double): JRandom = {
    /** Epsilon slop to avoid failure from floating point jitter */
    require(
      fraction >= (0.0 - RandomSampler.roundingEpsilon)
        && fraction <= (1.0 + RandomSampler.roundingEpsilon),
      s"Sampling fractions must be on interval [0, 1]")
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

private[scio] class PoissonValueSampler[K, V]()
  extends RandomValueSampler[K, V, IntegerDistribution] {

  // TODO: is it necessary to setSeed for each instance like Spark does?
  override def init(fraction: Double): IntegerDistribution = {
    /** Epsilon slop to avoid failure from floating point jitter. */
    require(
      fraction >= (0.0 - RandomSampler.roundingEpsilon),
      s"Sampling fractions must be >= 0")
    val r = new PoissonDistribution(if (fraction > 0.0) fraction else 1.0)
    if (seed > 0) {
      r.reseedRandomGenerator(seed)
    }
    r
  }

  override def samples(fraction: Double, rng: IntegerDistribution): Int =
    if (fraction <= 0.0) 0 else rng.sample()

}

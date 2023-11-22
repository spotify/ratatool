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

import com.spotify.scio.Random.{BernoulliValueSampler, RandomValueAssigner, RandomValueSampler}
import com.spotify.scio.values.{SCollection, SideInput}
import com.spotify.scio.coders.Coder
import org.apache.beam.sdk.transforms.ParDo
import org.slf4j.{Logger, LoggerFactory}

import scala.language.higherKinds
import scala.reflect.ClassTag
import scala.math._

object SamplerSCollectionFunctions {
  // TODO: What is a good number for tolerance
  private val errorTolerance = 1e-2

  private[samplers] def logDistributionDiffs[U: ClassTag](
    s: SCollection[(Double, Map[U, Double])],
    logger: Logger
  ): Unit = {
    s.map { case (totalDiff, keyDiffs) =>
      logger.info(s"Total record count differs from expected target count by: ${totalDiff * 100}%")
      keyDiffs.foreach { case (k, diff) =>
        logger.info(s"Count for key $k differs from expected target count by: ${diff * 100}%")
      }
    }
  }

  private[samplers] def assignRandomRoll[T: ClassTag: Coder, U: ClassTag: Coder](
    s: SCollection[T],
    keyFn: T => U
  ) =
    s.keyBy(keyFn).applyTransform(ParDo.of(new RandomValueAssigner[U, T]))

  private[samplers] def buildStratifiedDiffs[T: ClassTag: Coder, U: ClassTag: Coder](
    s: SCollection[T],
    sampled: SCollection[(U, T)],
    keyFn: T => U,
    prob: Double,
    exact: Boolean = false
  ): SCollection[(Double, Map[U, Double])] = {
    val targets = s
      .map(t => (1L, Map[U, Long](keyFn(t) -> 1L)))
      .sum
      .map { case (total, m) =>
        (total * prob, m.map { case (k, v) => (k, v * prob) })
      }
      .asSingletonSideInput

    sampled.keys
      .map(k => (1L, Map[U, Long](k -> 1L)))
      .sum
      .withSideInputs(targets)
      .map { case (res, sic) =>
        val (targetTotal, keyTargets) = sic(targets)
        val (totalCount, keyCounts) = res
        val totalDiff = (targetTotal - totalCount) / targetTotal
        val keyDiffs = keyTargets.keySet
          .map(k => k -> (keyTargets(k) - keyCounts.getOrElse(k, 0L)) / keyTargets(k))
          .toMap

        if (exact) {
          if (totalDiff > errorTolerance) {
            throw new Exception(
              s"Total elements sampled off by ${totalDiff * 100}% (> ${errorTolerance * 100}%)"
            )
          }
          keyDiffs.foreach { case (k, diff) =>
            if (diff > errorTolerance) {
              throw new Exception(
                s"Elements for key $k sample off by ${diff * 100}% (> ${errorTolerance * 100}%)"
              )
            }
          }
        }
        (totalDiff, keyDiffs)
      }
      .toSCollection
  }

  private[samplers] def buildUniformDiffs[T: ClassTag: Coder, U: ClassTag: Coder](
    s: SCollection[T],
    sampled: SCollection[(U, T)],
    keyFn: T => U,
    prob: Double,
    popPerKey: SideInput[Double],
    exact: Boolean = false
  ): SCollection[(Double, Map[U, Double])] = {
    sampled.keys
      .map(k => (1L, Map[U, Long](k -> 1L)))
      .sum
      .withSideInputs(popPerKey)
      .map { case (res, sic) =>
        val pop = sic(popPerKey)
        val (totalCount, keyCounts) = res
        val totalDiff = ((pop * keyCounts.size) - totalCount) / (pop * keyCounts.size)
        val keyDiffs =
          keyCounts.keySet.map(k => k -> (pop - keyCounts.getOrElse(k, 0L)) / pop).toMap

        if (exact) {
          if (totalDiff > errorTolerance) {
            throw new Exception(
              s"Total elements sampled off by ${totalDiff * 100}% (> ${errorTolerance * 100}%)"
            )
          }
          keyDiffs.foreach { case (k, diff) =>
            if (diff > errorTolerance) {
              throw new Exception(
                s"Elements for key $k sample off by ${diff * 100}% (> ${errorTolerance * 100}%)"
              )
            }
          }
        }
        (totalDiff, keyDiffs)
      }
      .toSCollection
  }

  private[samplers] def uniformParams[T: ClassTag: Coder, U: ClassTag: Coder](
    s: SCollection[T],
    keyFn: T => U,
    prob: Double
  ): (SideInput[Double], SCollection[(U, Double)]) = {
    val keyed = s.keyBy(keyFn)
    val keys = keyed.keys.distinct
    val keyCount = keys.count.asSingletonSideInput
    val totalRecords = s.count
    val populationPerKey: SideInput[Double] = totalRecords
      .withSideInputs(keyCount)
      .map { case (c, sic) => (c * prob) / sic(keyCount) }
      .toSCollection
      .asSingletonSideInput
    val probPerKey = keyed.countByKey
      .withSideInputs(populationPerKey)
      .map { case ((k, c), sic) =>
        (k, min(sic(populationPerKey) / c, 1.0))
      }
      .toSCollection
    (populationPerKey, probPerKey)
  }

  /**
   * Using Chernoff bounds we can find a `p` such that it is unlikely to have less than n * f
   * elements in our sample. This is similar to the calculation that Spark uses.
   *
   * Derivation: https://gist.github.com/mfranberg/a2eb63cf3f39c995b55f1bb3b4b7c51b
   *
   * @param n
   *   Number of elements we are sampling from
   * @param f
   *   Fraction of elements to sample
   * @param delta
   *   A parameter to tune how wide the bounds should be
   * @return
   *   A `p` such that count(Bernoulli trials < `p`) > n * f
   */
  private def getUpperBound(n: Long, f: Double, delta: Double): Double = {
    val gamma = -log(delta) / n
    f + gamma + sqrt(pow(gamma, 2) + (2 * gamma * f))
  }

  private def getLowerBound(n: Long, f: Double, delta: Double): Double = {
    val gamma = -(3 * log(delta)) / (2 * n)
    min(1.0, f + gamma - sqrt(pow(gamma, 2) + (2 * gamma * f)))
  }

  private def filterByThreshold[T: ClassTag: Coder, U: ClassTag: Coder](
    s: SCollection[(U, (T, Double))],
    thresholdByKey: SCollection[(U, Double)]
  ): SCollection[(U, T)] = {
    s.hashJoin(thresholdByKey)
      .filter { case (_, ((_, d), t)) => d <= t }
      .map { case (k, ((v, _), _)) => (k, v) }
  }

  private def stratifiedThresholdByKey[T: ClassTag: Coder, U: ClassTag: Coder](
    s: SCollection[(U, (T, Double))],
    prob: Double,
    delta: Double,
    sizePerKey: Int
  ): SCollection[(U, Double)] = {
    val countByKey = s.countByKey
    val targetByKey = countByKey.map { case (k, c) => (k, (c * prob).toLong) }
    val boundsByKey = countByKey
      .map { case (k, c) =>
        (k, (getLowerBound(c, prob, delta), getUpperBound(c, prob, delta)))
      }

    val boundCountsByKey = s
      .map { case (k, (_, d)) => (k, d) }
      .hashJoin(boundsByKey)
      .filter { case (_, (d, (_, u))) => d < u }
      .map { case (k, (d, (l, _))) => (k, (if (d < l) 1L else 0L, 1L)) }
      .sumByKey

    s.map { case (k, (_, d)) => (k, d) }
      .hashJoin(countByKey)
      .filter { case (_, (d, c)) =>
        d < getUpperBound(c, prob, delta) && d >= getLowerBound(c, prob, delta)
      }
      .map { case (k, (d, _)) => (k, d) }
      // TODO: Clean up magic number
      .topByKey(sizePerKey)(Ordering.by(identity[Double]).reverse)
      .hashJoin(boundCountsByKey)
      .hashJoin(boundsByKey)
      .hashJoin(targetByKey)
      .map { case (k, (((itr, (lCounts, uCounts)), (l, u)), target)) =>
        if (lCounts >= target) {
          (k, l)
        } else if (uCounts < target) {
          (k, u)
        } else {
          val threshold = itr.drop(max(0, (target - lCounts).toInt)).headOption
          (k, threshold.getOrElse(u))
        }
      }
  }

  private def uniformThresholdByKey[T: ClassTag: Coder, U: ClassTag: Coder](
    s: SCollection[(U, (T, Double))],
    probByKey: SCollection[(U, Double)],
    popPerKey: SideInput[Double],
    delta: Double,
    sizePerKey: Int
  ): SCollection[(U, Double)] = {
    val countByKey = s.countByKey
    val boundsByKey = probByKey
      .hashJoin(countByKey)
      .map { case (k, (p, c)) => (k, (getLowerBound(c, p, delta), getUpperBound(c, p, delta))) }

    val boundCountsByKey = s
      .map { case (k, (_, d)) => (k, d) }
      .hashJoin(boundsByKey)
      .filter { case (_, (d, (_, u))) => d < u }
      .map { case (k, (d, (l, _))) => (k, (if (d < l) 1L else 0L, 1L)) }
      .sumByKey

    s.map { case (k, (_, d)) => (k, d) }
      .hashJoin(boundsByKey)
      .filter { case (_, (d, (l, u))) => d >= l && d < u }
      .map { case (k, (d, _)) => (k, d) }
      // TODO: Clean up magic number
      .topByKey(sizePerKey)(Ordering.by(identity[Double]).reverse)
      .hashJoin(boundCountsByKey)
      .hashJoin(boundsByKey)
      .withSideInputs(popPerKey)
      .map { case ((k, ((itr, (lCounts, uCounts)), (l, u))), sic) =>
        if (lCounts >= sic(popPerKey)) {
          (k, l)
        } else if (uCounts < sic(popPerKey)) {
          (k, u)
        } else {
          val threshold = itr.drop(max(0, (sic(popPerKey) - lCounts).toInt)).headOption
          (k, threshold.getOrElse(u))
        }
      }
      .toSCollection
  }

  implicit class RatatoolKVDSCollection[T: ClassTag, U: ClassTag](
    s: SCollection[(U, (T, Double))]
  ) {

    /**
     * Performs higher precision sampling for a given distribution by assigning random probabilities
     * to each element in SCollection[T] and then finding the Kth lowest, where K is the target
     * population for a given strata or key.
     */
    def exactSampleDist(
      dist: SampleDistribution,
      keyFn: T => U,
      prob: Double,
      maxKeySize: Int,
      delta: Double = 1e-3
    )(implicit coder0: Coder[T], coder1: Coder[U]): SCollection[T] = {
      @transient lazy val logSerDe = LoggerFactory.getLogger(this.getClass)

      val (sampled, sampledDiffs) = dist match {
        case StratifiedDistribution =>
          val thresholdByKey = stratifiedThresholdByKey(s, prob, delta, maxKeySize)
          val sample = filterByThreshold(s, thresholdByKey)
          val diffs = buildStratifiedDiffs(s.values.keys, sample, keyFn, prob, exact = true)
          (sample, diffs)

        case UniformDistribution =>
          val (popPerKey, probPerKey) = uniformParams(s.values.keys, keyFn, prob)
          val thresholdByKey = uniformThresholdByKey(s, probPerKey, popPerKey, delta, maxKeySize)
          val sample = filterByThreshold(s, thresholdByKey)
          val diffs = buildUniformDiffs(s.values.keys, sample, keyFn, prob, popPerKey, exact = true)
          (sample, diffs)
      }
      logDistributionDiffs(sampledDiffs, logSerDe)
      sampled.values
    }
  }

  implicit class RatatoolSCollection[T: ClassTag](s: SCollection[T]) {

    /**
     * Wrapper function that samples an SCollection[T] for a given distribution. This sampling is
     * done approximately using a Bernoulli probability (coin toss) per element. For more precision
     * sampling use `exactSampleDist` instead
     */
    def sampleDist[U: ClassTag: Coder](dist: SampleDistribution, keyFn: T => U, prob: Double)(
      implicit coder: Coder[T]
    ): SCollection[T] = {
      @transient lazy val logSerDe = LoggerFactory.getLogger(this.getClass)

      val (sampled, sampledDiffs) = dist match {
        case StratifiedDistribution =>
          val sampleFn: RandomValueSampler[U, T, _] = new BernoulliValueSampler[U, T]
          val keyed = s.keyBy(keyFn)
          val sample = keyed.map((_, prob)).applyTransform(ParDo.of(sampleFn))
          val diffs = buildStratifiedDiffs(s, sample, keyFn, prob)
          (sample, diffs)

        case UniformDistribution =>
          val sampleFn: RandomValueSampler[U, T, _] = new BernoulliValueSampler[U, T]
          val (popPerKey, probPerKey) = uniformParams(s, keyFn, prob)
          val sample = s
            .keyBy(keyFn)
            .hashJoin(probPerKey)
            .map { case (k, (v, keyProb)) => ((k, v), keyProb) }
            .applyTransform(ParDo.of(sampleFn))

          val diffs = buildUniformDiffs(s, sample, keyFn, prob, popPerKey)
          (sample, diffs)
      }

      logDistributionDiffs(sampledDiffs, logSerDe)
      sampled.values
    }
  }
}

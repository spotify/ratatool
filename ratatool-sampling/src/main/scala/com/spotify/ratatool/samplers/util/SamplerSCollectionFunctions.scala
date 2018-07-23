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
import org.apache.beam.sdk.transforms.ParDo
import org.slf4j.{Logger, LoggerFactory}

import scala.reflect.ClassTag
import scala.math.max

object SamplerSCollectionFunctions {

  private[samplers] def logDistributionDiffs[U: ClassTag](s: SCollection[(Double, Map[U, Double])],
                                                           logger: Logger): Unit = {
    s.map{ case (totalDiff, keyDiffs) =>
      logger.info(s"Total record counts differs from expected target count by: $totalDiff%")
      keyDiffs.foreach{ case (k, diff) =>
        logger.info(s"Count for key $k differs from expected target count by: $diff%")
      }
    }
  }

  private[samplers] def assignRandomRoll[T: ClassTag, U: ClassTag](s: SCollection[T],
                                                                   keyFn: T => U) = {
    s.keyBy(keyFn).applyTransform(ParDo.of(new RandomValueAssigner[U, T]))
  }

  private[samplers] def buildStratifiedDiffs[T: ClassTag, U: ClassTag](s: SCollection[T],
                                                                       sampled: SCollection[(U, T)],
                                                                       keyFn: T => U,
                                                                       prob: Double)
  : SCollection[(Double, Map[U, Double])] = {
    val targets = s.map(t => (1L, Map[U, Long](keyFn(t) -> 1L))).sum
      .map{case (total, m) =>
        (total * prob,
          m.map{ case (k, v) => (k, v * prob)})}.asSingletonSideInput

    sampled.keys.map(k => (1L, Map[U, Long](k -> 1L))).sum
      .withSideInputs(targets).map{case (res, sic) =>
      val (targetTotal, keyTargets) = sic(targets)
      val (totalCount, keyCounts) = res
      val totalDiff = (targetTotal - totalCount)/targetTotal
      val keyDiffs = keyTargets.keySet.map(k =>
          k -> (keyTargets(k) - keyCounts.getOrElse(k, 0L))/keyTargets(k)).toMap

      (totalDiff, keyDiffs)
    }.toSCollection
  }

  private[samplers] def buildUniformDiffs[T: ClassTag, U: ClassTag](s: SCollection[T],
                                                                    sampled: SCollection[(U, T)],
                                                                    keyFn: T => U,
                                                                    prob: Double,
                                                                    popPerKey: SideInput[Double])
  : SCollection[(Double, Map[U, Double])] = {
    sampled.keys.map(k => (1L, Map[U, Long](k -> 1L))).sum
      .withSideInputs(popPerKey).map{ case (res, sic) =>
        val popPerKey = sic(popPerKey)
        val (totalCount, keyCounts) = res
        val totalDiff = ((popPerKey * keyCounts.size) - totalCount)/(popPerKey * keyCounts.size)
        val keyDiffs = keyCounts.keySet.map(k =>
          k -> (popPerKey - keyCounts.getOrElse(k, 0L))/popPerKey).toMap

        (totalDiff, keyDiffs)
    }.toSCollection
  }

  private[samplers] def uniformParams[T: ClassTag, U: ClassTag](s: SCollection[T],
                                                                keyFn: T => U,
                                                                prob: Double)
  : (SideInput[Double], SCollection[(U, Double)]) = {
    val keyed = s.keyBy(keyFn)
    val keys = keyed.keys.distinct
    val keyCount = keys.count.asSingletonSideInput
    val totalRecords = s.count
    val populationPerKey: SideInput[Double] = totalRecords.withSideInputs(keyCount)
      .map{case (c, sic) => (c * prob)/sic(keyCount)}.toSCollection.asSingletonSideInput
    val probPerKey = keyed.countByKey.withSideInputs(populationPerKey).map {
      case ((k, c), sic) => (k, sic(populationPerKey)/c) }.toSCollection
    (populationPerKey, probPerKey)
  }

  private def filterByThreshold[T: ClassTag, U: ClassTag](s: SCollection[(U, (T, Double))],
                                                           thresholdByKey: SCollection[(U, Double)])
  : SCollection[(U, T)] = {
    s.join(thresholdByKey)
      .filter{case (_, ((_, d), t)) => d < t}
      .map{case (k, ((v, _), _)) => (k, v)}
  }

  private def stratifiedThresholdByKey[T: ClassTag, U: ClassTag](
      s: SCollection[(U, (T, Double))],
      countsByKey: SCollection[(U, (Long, Long))],
      targetByKey: SCollection[(U, Long)],
      variance: Double,
      prob: Double)
  : SCollection[(U, Double)] = {
    s.map{case (k, (_, d)) => (k, d)}
      .filter{case (_, d) => d < (prob + variance) && d > (prob - variance)}
      //TODO: Clean up magic number
      .topByKey(1e8.toInt)(Ordering.by(identity[Double]).reverse)
      .hashJoin(countsByKey)
      .hashJoin(targetByKey)
      .map { case (k, ((itr, (lower, upper)), target)) =>
        if (lower >= target) {
          (k, prob - variance)
        }
        else if (upper < target) {
        (k, prob + variance)
      }
      else {
        (k, itr.drop((target - lower).toInt).headOption.getOrElse(prob + variance))
      }
    }
  }

  private def uniformThresholdByKey[T: ClassTag, U: ClassTag](
                                                        s: SCollection[(U, (T, Double))],
                                                        countsByKey: SCollection[(U, (Long, Long))],
                                                        varByKey: SCollection[(U, Double)],
                                                        probByKey: SCollection[(U, Double)],
                                                        popPerKey: SideInput[Double])
  : SCollection[(U, Double)] = {
    s.map{case (k, (_, d)) => (k, d)}
      .hashJoin(varByKey)
      .hashJoin(probByKey)
      .filter{case (_, ((d, variance), prob)) =>
        d < (prob + variance) && d >= (prob - variance)}
      .map{case (k, ((d, _), _)) => (k, d)}
      //TODO: Clean up magic number
      .topByKey(1e8.toInt)(Ordering.by(identity[Double]).reverse)
      .join(countsByKey)
      .hashJoin(varByKey)
      .hashJoin(probByKey)
      .withSideInputs(popPerKey)
      .map { case ((k, ((((itr, (lower, upper)), variance)), prob)), sic) =>
        if (lower >= sic(popPerKey)) {
          (k, prob - variance)
        }
        else if (upper < sic(popPerKey)) {
          (k, prob + variance)
        }
        else {
          val threshold = itr.drop(max(0, (sic(popPerKey) - lower).toInt)).headOption
          (k, threshold.getOrElse(prob + variance))
        }
      }.toSCollection
  }

  implicit class RatatoolKVDSCollection[T: ClassTag, U: ClassTag](s: SCollection[(U, (T, Double))]){
    def exactStratifiedSample(keyFn: T => U,
                              prob: Double)
    : SCollection[T] = {
      @transient lazy val logSerDe = LoggerFactory.getLogger(this.getClass)

      val targets = s.countByKey.map{case (k, c) => (k, (c * prob).toLong)}
      val variance = prob * (1 - prob)
      val counts = s.filter{case (_, (_, d)) => d < (prob + variance)}
        .map{ case (k, (_, d)) => (k, (if (d < prob - variance) 1L else 0L, 1L))}
        .sumByKey

      val thresholdByKey = stratifiedThresholdByKey(s, counts, targets, variance, prob)

      val sampled = filterByThreshold(s, thresholdByKey)
      val sampledDiffs = buildStratifiedDiffs(s, sampled, keyFn, prob)
      logDistributionDiffs(sampledDiffs, logSerDe)
      sampled.values
    }

    def exactUniformSample(keyFn: T => U,
                           prob: Double
                          ): SCollection[T] = {
      @transient lazy val logSerDe = LoggerFactory.getLogger(this.getClass)
      val (popPerKey, probPerKey) = uniformParams(s, keyFn, prob)
      //TODO: Find better bounds than variance, potentially what Spark uses
      val variancePerKey = probPerKey.map{case (k, f) => (k, f * (1 - f))}

      val counts = s
        .hashJoin(variancePerKey)
        .hashJoin(probPerKey)
        .filter{case (_, (((_, d), variance), keyProb)) => d < (keyProb + variance)}
        .map{ case (k, (((_, d), variance), keyProb))  =>
          (k, (if (d < (keyProb - variance)) 1L else 0L, 1L))}
        .sumByKey

      val thresholdByKey = uniformThresholdByKey(s, counts, variancePerKey, probPerKey, popPerKey)

      val sampled = filterByThreshold(s, thresholdByKey)

      val sampledDiffs = buildUniformDiffs(s, sampled, keyFn, prob, popPerKey)
      logDistributionDiffs(sampledDiffs, logSerDe)
      sampled.values
    }
  }


  //scalastyle:off cyclomatic.complexity method.length
  implicit class RatatoolSCollection[T: ClassTag](s: SCollection[T]) {
    def stratifiedSample[U: ClassTag](keyFn: T => U,
                                      withReplacement: Boolean,
                                      prob: Double
                                     ): SCollection[T] = {
      @transient lazy val logSerDe = LoggerFactory.getLogger(this.getClass)

      val sampleFn: RandomValueSampler[U, T, _] =
        if (withReplacement){
          throw new UnsupportedOperationException(
            "Sampling with replacement not currently supported for distributions")
        } else {
          new BernoulliValueSampler[U, T]
        }
      val keyed = s.keyBy(keyFn)
      val sampled = keyed.map((_, prob)).applyTransform(ParDo.of(sampleFn))
      val sampledDiffs = buildStratifiedDiffs(s, sampled, keyFn, prob)
      logDistributionDiffs(sampledDiffs, logSerDe)
      sampled.values
    }
    //scalastyle:on

    def uniformSample[U: ClassTag](keyFn: T => U,
                                   withReplacement: Boolean,
                                   prob: Double
                                  ): SCollection[T] = {
      @transient lazy val logSerDe = LoggerFactory.getLogger(this.getClass)

      val sampleFn: RandomValueSampler[U, T, _] =
        if (withReplacement){
          throw new UnsupportedOperationException(
            "Sampling with replacement not currently supported for distributions")
        } else {
          new BernoulliValueSampler[U, T]
        }
      val (popPerKey, probPerKey) = uniformParams(s, keyFn, prob)
      val sampled = s.keyBy(keyFn)
        .join(probPerKey).map{ case (k, (v, keyProb)) =>
        ((k, v), keyProb)}.applyTransform(ParDo.of(sampleFn))

      val sampledDiffs = buildUniformDiffs(s, sampled, keyFn, prob, popPerKey)
      logDistributionDiffs(sampledDiffs, logSerDe)
      sampled.values
    }
  }
}


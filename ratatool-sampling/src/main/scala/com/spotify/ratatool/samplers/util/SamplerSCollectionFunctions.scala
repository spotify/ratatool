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

  private[samplers] def logDistributionDiffs[U: ClassTag](
                                             coll: SCollection[(Double, Map[U, Double])],
                                             logger: Logger): Unit = {
    coll.map{ case (totalDiff, keyDiffs) =>
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
                                                                       fraction: Double)
  : SCollection[(Double, Map[U, Double])] = {
    val targets = s.map(t => (1L, Map[U, Long](keyFn(t) -> 1L))).sum
      .map{case (total, m) =>
        (total * fraction,
          m.map{ case (k, v) => (k, v * fraction)})}.asSingletonSideInput

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

  private[samplers] def buildUniformDiffs[T: ClassTag, U: ClassTag](original: SCollection[T],
                                                sampled: SCollection[(U, T)],
                                                keyFn: T => U,
                                                fraction: Double,
                                                populationPerKey: SideInput[Double])
  : SCollection[(Double, Map[U, Double])] = {
    sampled.keys.map(k => (1L, Map[U, Long](k -> 1L))).sum
      .withSideInputs(populationPerKey).map{ case (res, sic) =>
        val ppk = sic(populationPerKey)
        val (totalCount, keyCounts) = res
        val totalDiff = ((ppk * keyCounts.size) - totalCount)/(ppk * keyCounts.size)
        val keyDiffs = keyCounts.keySet.map(k =>
          k -> (ppk - keyCounts.getOrElse(k, 0L))/ppk).toMap

        (totalDiff, keyDiffs)
    }.toSCollection
  }

  private[samplers] def uniformParams[T: ClassTag, U: ClassTag](s: SCollection[T],
                                                                keyFn: T => U,
                                                                fraction: Double)
  : (SideInput[Double], SCollection[(U, Double)]) = {
    val keyed = s.keyBy(keyFn)
    val keys = keyed.keys.distinct
    val keyCount = keys.count.asSingletonSideInput
    val totalRecords = s.count
    val populationPerKey: SideInput[Double] = totalRecords.withSideInputs(keyCount)
      .map{case (c, sic) => (c * fraction)/sic(keyCount)}.toSCollection.asSingletonSideInput
    val fractionPerKey = keyed.countByKey.withSideInputs(populationPerKey).map {
      case ((k, c), sic) => (k, sic(populationPerKey)/c) }.toSCollection
    (populationPerKey, fractionPerKey)
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
      fraction: Double)
  : SCollection[(U, Double)] = {
    s.map{case (k, (_, d)) => (k, d)}
      .filter{case (_, d) => d < (fraction + variance) && d > (fraction - variance)}
      //TODO: Clean up magic number
      .topByKey(1e8.toInt)(Ordering.by(identity[Double]).reverse)
      .hashJoin(countsByKey)
      .hashJoin(targetByKey)
      .map { case (k, ((itr, (lower, upper)), target)) =>
      if (upper < target) {
        (k, fraction + variance)
      }
      else {
        (k, itr.drop((target - lower).toInt).headOption.getOrElse(fraction + variance))
      }
    }
  }

  private def uniformThresholdByKey[T: ClassTag, U: ClassTag](
                                                        s: SCollection[(U, (T, Double))],
                                                        countsByKey: SCollection[(U, (Long, Long))],
                                                        varByKey: SCollection[(U, Double)],
                                                        fractionByKey: SCollection[(U, Double)],
                                                        popPerKey: SideInput[Double])
  : SCollection[(U, Double)] = {
    s.map{case (k, (_, d)) => (k, d)}
      .hashJoin(varByKey)
      .hashJoin(fractionByKey)
      .filter{case (_, ((d, variance), fraction)) =>
        d < (fraction + variance) && d >= (fraction - variance)}
      .map{case (k, ((d, _), _)) => (k, d)}
      //TODO: Clean up magic number
      .topByKey(1e8.toInt)(Ordering.by(identity[Double]).reverse)
      .join(countsByKey)
      .hashJoin(varByKey)
      .hashJoin(fractionByKey)
      .withSideInputs(popPerKey)
      .map { case ((k, ((((itr, (lower, upper)), variance)), fraction)), sic) =>
        //TODO: Check the min here
        if (upper < sic(popPerKey)) {
          (k, fraction + variance)
        }
        else {
          val threshold = itr.drop(max(0, (sic(popPerKey) - lower).toInt)).headOption
          (k, threshold.getOrElse(fraction + variance))
        }
      }.toSCollection
  }

  implicit class RatatoolKVDSCollection[T: ClassTag, U: ClassTag](s: SCollection[(U, (T, Double))]){
    def exactStratifiedSample(keyFn: T => U,
                              fraction: Double
                             ): SCollection[T] = {
      @transient lazy val logSerDe = LoggerFactory.getLogger(this.getClass)

      val targets = s.countByKey.map{case (k, c) => (k, (c * fraction).toLong)}
      val variance = fraction * (1 - fraction)
      //TODO: Find better cutoffs than +/- variance
      val counts = s.filter{case (_, (_, d)) => d < (fraction + variance)}
        .map{ case (k, (_, d)) => (k, (if (d < fraction - variance) 1L else 0L, 1L))}
        .sumByKey

      val thresholdByKey = stratifiedThresholdByKey(s, counts, targets, variance, fraction)

      val sampled = filterByThreshold(s, thresholdByKey)
      val sampledDiffs = buildStratifiedDiffs(s, sampled, keyFn, fraction)
      logDistributionDiffs(sampledDiffs, logSerDe)
      sampled.values
    }

    def exactUniformSample(keyFn: T => U,
                           fraction: Double
                          ): SCollection[T] = {
      @transient lazy val logSerDe = LoggerFactory.getLogger(this.getClass)
      val (ppk, fpk) = uniformParams(s, keyFn, fraction)
      //TODO: Find better bounds than variance, potentially what Spark uses
      val variancePerKey = fpk.map{case (k, f) => (k, f * (1 - f))}

      val counts = s
        .hashJoin(variancePerKey)
        .hashJoin(fpk)
        .filter{case (_, (((_, d), variance), keyFraction)) => d < (keyFraction + variance)}
        .map{ case (k, (((_, d), variance), keyFraction))  =>
          (k, (if (d < (keyFraction - variance)) 1L else 0L, 1L))}
        .sumByKey

      val thresholdByKey = uniformThresholdByKey(s, counts, variancePerKey, fpk, ppk)

      val sampled = filterByThreshold(s, thresholdByKey)

      val sampledDiffs = buildUniformDiffs(s, sampled, keyFn, fraction, ppk)
      logDistributionDiffs(sampledDiffs, logSerDe)
      sampled.values
    }
  }


  //scalastyle:off cyclomatic.complexity method.length
  implicit class RatatoolSCollection[T: ClassTag](s: SCollection[T]) {
    def stratifiedSample[U: ClassTag](keyFn: T => U,
                                      withReplacement: Boolean,
                                      fraction: Double
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
      val sampled = keyed.map((_, fraction)).applyTransform(ParDo.of(sampleFn))
      val sampledDiffs = buildStratifiedDiffs(s, sampled, keyFn, fraction)
      logDistributionDiffs(sampledDiffs, logSerDe)
      sampled.values
    }
    //scalastyle:on

    def uniformSample[U: ClassTag](keyFn: T => U,
                                   withReplacement: Boolean,
                                   fraction: Double
                                  ): SCollection[T] = {
      @transient lazy val logSerDe = LoggerFactory.getLogger(this.getClass)

      val sampleFn: RandomValueSampler[U, T, _] =
        if (withReplacement){
          throw new UnsupportedOperationException(
            "Sampling with replacement not currently supported for distributions")
        } else {
          new BernoulliValueSampler[U, T]
        }
      val (ppk, fpk) = uniformParams(s, keyFn, fraction)
      val sampled = s.keyBy(keyFn)
        .join(fpk).map{ case (k, (v, keyFraction)) =>
        ((k, v), keyFraction)}.applyTransform(ParDo.of(sampleFn))

      val sampledDiffs = buildUniformDiffs(s, sampled, keyFn, fraction, ppk)
      logDistributionDiffs(sampledDiffs, logSerDe)
      sampled.values
    }
  }
}


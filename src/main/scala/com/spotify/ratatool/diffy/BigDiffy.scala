/*
 * Copyright 2016 Spotify AB.
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

package com.spotify.ratatool.diffy

import java.net.URI

import com.google.api.services.bigquery.model.{TableFieldSchema, TableRow, TableSchema}
import com.google.protobuf.GeneratedMessage
import com.spotify.ratatool.GcsConfiguration
import com.spotify.ratatool.samplers.AvroSampler
import com.spotify.scio._
import com.spotify.scio.bigquery.BigQueryClient
import com.spotify.scio.values.SCollection
import com.twitter.algebird._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * Diff type between two records of the same key.
 *
 * SAME - the two records are identical.
 * DIFFERENT - the two records are different.
 * MISSING_LHS - left hand side record is missing.
 * MISSING_RHS - right hand side record is missing.
 */
object DiffType extends Enumeration {
  val SAME, DIFFERENT, MISSING_LHS, MISSING_RHS = Value
}

/**
 * Key level statistics.
 *
 * key - primary being compared.
 * diffType - how the two records of the given key compares.
 */
case class KeyStats(key: String, diffType: DiffType.Value) {
  override def toString: String = s"$key\t$diffType"
}

/**
 * Global level statistics.
 *
 * numTotal - number of total unique keys.
 * numSame - number of keys with same records on both sides.
 * numDiff - number of keys with different records on both sides.
 * numMissingLhs - number of keys with missing left hand side record.
 * numMissingRhs - number of keys with missing right hand side record.
 */
case class GlobalStats(numTotal: Long, numSame: Long, numDiff: Long,
                       numMissingLhs: Long, numMissingRhs: Long) {
  override def toString: String = s"$numTotal\t$numSame\t$numDiff\t$numMissingLhs\t$numMissingRhs"
}

/** Delta level statistics. */
case class DeltaStats(deltaType: DeltaType.Value,
                      min: Double, max: Double, count: Long,
                      mean: Double, variance: Double, stddev: Double,
                      skewness: Double, kurtosis: Double) {
  override def toString: String =
    s"$deltaType\t$min\t$max\t$count\t$mean\t$variance\t$stddev\t$skewness\t$kurtosis"
}

/**
 * Field level statistics.
 *
 * field - "." separated field identifier.
 * count - number of records with different values of the given field.
 * fraction - fraction over total number of keys with different records on both sides.
 * deltaStats - statistics of field value deltas.
 */
case class FieldStats(field: String,
                      count: Long,
                      fraction: Double,
                      deltaStats: Option[DeltaStats]) {
  override def toString: String =
    s"$field\t$count\t$fraction\t" + deltaStats.map(d => "\t" + d).getOrElse("")
}

/** Big diff between two data sets given a primary key. */
class BigDiffy[T](lhs: SCollection[T], rhs: SCollection[T],
                  d: Diffy[T], keyFn: T => String) {

  private lazy val deltas: SCollection[(String, (Seq[Delta], DiffType.Value))] =
    BigDiffy.computeDeltas(lhs, rhs, d, keyFn)

  private lazy val globalAndFieldStats: SCollection[(GlobalStats, Iterable[FieldStats])] =
    BigDiffy.computeGlobalAndFieldStats(deltas)

  /** Global level statistics. */
  lazy val globalStats: SCollection[GlobalStats] = globalAndFieldStats.keys

  /** Key level statistics. */
  lazy val keyStats: SCollection[KeyStats] = deltas.map { case (k, (_, dt)) => KeyStats(k, dt) }

  /** Field level statistics. */
  lazy val fieldStats: SCollection[FieldStats] = globalAndFieldStats.flatMap(_._2)

}

/** Big diff between two data sets given a primary key. */
object BigDiffy {

  // (field, deltas, diff type)
  type DeltaSCollection = SCollection[(String, (Seq[Delta], DiffType.Value))]

  private def computeDeltas[T](lhs: SCollection[T], rhs: SCollection[T],
                               d: Diffy[T], keyFn: T => String): DeltaSCollection = {
    // extract keys and prefix records with L/R sub-key
    val lKeyed = lhs.map(t => (keyFn(t), ("l", t)))
    val rKeyed = rhs.map(t => (keyFn(t), ("r", t)))

    val sc = lhs.context
    val accSame = sc.sumAccumulator[Long]("SAME")
    val accDiff = sc.sumAccumulator[Long]("DIFFERENT")
    val accMissingLhs = sc.sumAccumulator[Long]("MISSING_LHS")
    val accMissingRhs = sc.sumAccumulator[Long]("MISSING_RHS")

    (lKeyed ++ rKeyed)
      .groupByKey
      .map { case (k, vs) =>
        val m = vs.toMap // L/R -> record
        if (m.size == 2) {
          val ds = d(m("l"), m("r"))
          val dt = if (ds.isEmpty) DiffType.SAME else DiffType.DIFFERENT
          (k, (ds, dt))
        } else {
          val dt = if (m.contains("l")) DiffType.MISSING_RHS else DiffType.MISSING_LHS
          (k, (Nil, dt))
        }
      }
      .withAccumulator(accSame, accDiff, accMissingLhs, accMissingRhs)
      .map { (x, c) =>
        x._2._2 match {
          case DiffType.SAME => c.addValue(accSame, 1L)
          case DiffType.DIFFERENT => c.addValue(accDiff, 1L)
          case DiffType.MISSING_LHS => c.addValue(accMissingLhs, 1L)
          case DiffType.MISSING_RHS => c.addValue(accMissingRhs, 1L)
        }
        x
      }
      .toSCollection
  }

  private def computeGlobalAndFieldStats(deltas: DeltaSCollection)
  : SCollection[(GlobalStats, Iterable[FieldStats])] = {
    // Semigroup[DeltaType.Value] so it can be propagated during sum over Map
    implicit val deltaTypeSemigroup = new Semigroup[DeltaType.Value] {
      override def plus(l: DeltaType.Value, r: DeltaType.Value): DeltaType.Value = l
    }

    // Map value to be summed
    type MapVal = (Long, Option[(DeltaType.Value, Min[Double], Max[Double], Moments)])

    deltas
      .map { case (_, (ds, dt)) =>
        val m = mutable.Map.empty[String, MapVal]
        ds.foreach { d =>
          val optD = d.delta match {
            case UnknownDelta => None
            case TypedDelta(t, v) =>
              Some((t, Min(v), Max(v), Moments.aggregator.prepare(v)))
          }
          // Map of field -> (count, delta statistics)
          m(d.field) = (1L, optD)
        }
        // also sum global statistics
        val dtVec = dt match {
          case DiffType.SAME => (1L, 1L, 0L, 0L, 0L)
          case DiffType.DIFFERENT => (1L, 0L, 1L, 0L, 0L)
          case DiffType.MISSING_LHS => (1L, 0L, 0L, 1L, 0L)
          case DiffType.MISSING_RHS => (1L, 0L, 0L, 0L, 1L)
        }
        (dtVec, m.toMap)
      }
      .sum
      .map { case (dtVec, fieldMap) =>
        val globalKeyStats = GlobalStats.tupled(dtVec)
        val fieldStats = fieldMap.map { case (field, (count, optD)) =>
          val deltaStats = optD.map { d =>
            val (dt, min, max, m) = d
            DeltaStats(deltaType = dt, min = min.get, max = max.get, count = m.count,
              mean = m.mean, variance = m.variance, stddev = m.stddev,
              skewness = m.skewness, kurtosis = m.kurtosis)
          }
          val globalKeyStats = GlobalStats.tupled(dtVec)
          FieldStats(
            field, count, count.toDouble / globalKeyStats.numDiff, deltaStats)
        }
        (globalKeyStats, fieldStats)
      }
  }

  /** Diff two data sets. */
  def diff[T: ClassTag](lhs: SCollection[T], rhs: SCollection[T],
                        d: Diffy[T], keyFn: T => String): BigDiffy[T] =
    new BigDiffy[T](lhs, rhs, d, keyFn)

  /** Diff two Avro data sets. */
  def diffAvro[T <: GenericRecord : ClassTag](sc: ScioContext,
                                              lhs: String, rhs: String,
                                              keyFn: T => String,
                                              ignore: Set[String] = Set.empty,
                                              schema: Schema = null): BigDiffy[T] =
    diff(sc.avroFile[T](lhs, schema), sc.avroFile[T](rhs, schema), new AvroDiffy[T](ignore), keyFn)

  /** Diff two ProtoBuf data sets. */
  def diffProtoBuf[T <: GeneratedMessage : ClassTag](sc: ScioContext,
                                                     lhs: String, rhs: String,
                                                     keyFn: T => String,
                                                     ignore: Set[String] = Set.empty): BigDiffy[T] =
    diff(sc.protobufFile(lhs), sc.protobufFile(rhs), new ProtoBufDiffy[T](ignore), keyFn)

  /** Diff two TableRow data sets. */
  def diffTableRow(sc: ScioContext,
                   lhs: String, rhs: String,
                   keyFn: TableRow => String,
                   ignore: Set[String] = Set.empty): BigDiffy[TableRow] = {
    // TODO: handle schema evolution
    val bq = BigQueryClient.defaultInstance()
    val lSchema = bq.getTableSchema(lhs)
    val rSchema = bq.getTableSchema(rhs)
    val schema = mergeTableSchema(lSchema, rSchema)
    diff(sc.bigQueryTable(lhs), sc.bigQueryTable(rhs), new TableRowDiffy(schema, ignore), keyFn)
  }

  /** Merge two BigQuery TableSchemas. */
  def mergeTableSchema(x: TableSchema, y: TableSchema): TableSchema = {
    val r = new TableSchema
    r.setFields(mergeFields(x.getFields.asScala, y.getFields.asScala).asJava)
  }

  private def mergeFields(x: Seq[TableFieldSchema],
                          y: Seq[TableFieldSchema]): Seq[TableFieldSchema] = {
    val xMap = x.map(f => (f.getName, f)).toMap
    val yMap = x.map(f => (f.getName, f)).toMap
    val names = mutable.LinkedHashSet.empty[String]
    xMap.foreach(kv => names.add(kv._1))
    yMap.foreach(kv => names.add(kv._1))
    names
      .map { n =>
        (xMap.get(n), yMap.get(n)) match {
          case (Some(f), None) => f
          case (None, Some(f)) => f
          case (Some(fx), Some(fy)) =>
            assert(fx.getType == fy.getType && fx.getMode == fy.getMode)
            if (fx.getType == "RECORD") {
              fx.setFields(mergeFields(fx.getFields.asScala, fy.getFields.asScala).asJava)
            } else {
              fx
            }
          case _ => throw new RuntimeException
        }
      }
      .toSeq
  }

  /** Scio pipeline for BigDiffy. */
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val mode = args("mode")
    val lhs = args("left")
    val rhs = args("right")
    val key = args("key")
    val output = args("output")

    val result = mode match {
      case "avro" =>
        // TODO: handle schema evolution
        val fs = FileSystem.get(new URI(rhs), GcsConfiguration.get())
        val path = fs.globStatus(new Path(rhs)).head.getPath
        val schema = new AvroSampler(path).sample(1, true).head.getSchema
        BigDiffy.diffAvro[GenericRecord](sc, lhs, rhs, _.get(key).toString, schema = schema)
      case "bigquery" =>
        BigDiffy.diffTableRow(sc, lhs, rhs, _.get(key).toString)
      case m =>
        throw new IllegalArgumentException(s"mode $m not supported")
    }

    result.keyStats.saveAsTextFile(s"$output/keys")
    result.fieldStats.saveAsTextFile(s"$output/fields")
    result.globalStats.saveAsTextFile(s"$output/global")

    sc.close()
  }

}

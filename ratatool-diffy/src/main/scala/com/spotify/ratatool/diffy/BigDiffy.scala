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
import com.google.protobuf.AbstractMessage
import com.spotify.ratatool.GcsConfiguration
import com.spotify.ratatool.samplers.AvroSampler
import com.spotify.scio._
import com.spotify.scio.bigquery.BigQueryClient
import com.spotify.scio.io.Tap
import com.spotify.scio.values.SCollection
import com.twitter.algebird._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.io.TextIO
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.Future
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
                  diffy: Diffy[T], keyFn: T => String) {

  private lazy val _deltas: SCollection[(String, (Seq[Delta], DiffType.Value))] =
    BigDiffy.computeDeltas(lhs, rhs, diffy, keyFn)

  private lazy val globalAndFieldStats: SCollection[(GlobalStats, Iterable[FieldStats])] =
    BigDiffy.computeGlobalAndFieldStats(_deltas)

  /**
   * Key and field level delta.
   *
   * Output tuples are (key, field, LHS, RHS). Note that LHS and RHS may not be serializable.
   */
  lazy val deltas: SCollection[(String, String, Any, Any)] =
    _deltas.flatMap { case (k, (ds, dt)) =>
      ds.map(d => (k, d.field, d.left, d.right))
    }

  /** Global level statistics. */
  lazy val globalStats: SCollection[GlobalStats] = globalAndFieldStats.keys

  /** Key level statistics. */
  lazy val keyStats: SCollection[KeyStats] = _deltas.map { case (k, (_, dt)) => KeyStats(k, dt) }

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
    val accSame = ScioMetrics.counter[Long]("SAME")
    val accDiff = ScioMetrics.counter[Long]("DIFFERENT")
    val accMissingLhs = ScioMetrics.counter[Long]("MISSING_LHS")
    val accMissingRhs = ScioMetrics.counter[Long]("MISSING_RHS")

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
      .map { x =>
        x._2._2 match {
          case DiffType.SAME => accSame.inc()
          case DiffType.DIFFERENT => accDiff.inc()
          case DiffType.MISSING_LHS => accMissingLhs.inc()
          case DiffType.MISSING_RHS => accMissingRhs.inc()
        }
        x
      }
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
                                              diffy: AvroDiffy[T],
                                              schema: Schema = null): BigDiffy[T] =
    diff(sc.avroFile[T](lhs, schema), sc.avroFile[T](rhs, schema), diffy, keyFn)

  /** Diff two ProtoBuf data sets. */
  def diffProtoBuf[T <: AbstractMessage : ClassTag](sc: ScioContext,
                                                     lhs: String, rhs: String,
                                                     keyFn: T => String,
                                                     diffy: ProtoBufDiffy[T]): BigDiffy[T] =
    diff(sc.protobufFile(lhs), sc.protobufFile(rhs), diffy, keyFn)

  /** Diff two TableRow data sets. */
  def diffTableRow(sc: ScioContext,
                   lhs: String, rhs: String,
                   keyFn: TableRow => String,
                   diffy: TableRowDiffy): BigDiffy[TableRow] =
    diff(sc.bigQueryTable(lhs), sc.bigQueryTable(rhs), diffy, keyFn)

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

  private def usage(): Unit = {
    // scalastyle:off regex
    println(
      """BigDiffy - pair-wise field-level statistical diff
        |Usage: BigDiffy [dataflow_options] [options]
        |
        |  --mode=[avro|bigquery]
        |  --key=<key>            '.' separated key field
        |  --lhs=<path>           LHS File path or BigQuery table
        |  --rhs=<path>           RHS File path or BigQuery table
        |  --output=<output>      File path prefix for output
        |  --ignore=<keys>        ',' separated field list to ignore
        |  --unordered=<keys>     ',' separated field list to treat as unordered
      """.stripMargin)
    // scalastyle:on regex
    sys.exit(1)
  }

  private def avroKeyFn(key: String): GenericRecord => String = {
    @tailrec
    def get(xs: Array[String], i: Int, r: GenericRecord): String =
      if (i == xs.length - 1) {
        r.get(xs(i)).toString
      } else {
        get(xs, i + 1, r.get(xs(i)).asInstanceOf[GenericRecord])
      }
    val xs = key.split('.')
    (r: GenericRecord) => get(xs, 0, r)
  }

  private def tableRowKeyFn(key: String): TableRow => String = {
    @tailrec
    def get(xs: Array[String], i: Int, r: java.util.Map[String, AnyRef]): String =
      if (i == xs.length - 1) {
        r.get(xs(i)).toString
      } else {
        get(xs, i + 1, r.get(xs(i)).asInstanceOf[java.util.Map[String, AnyRef]])
      }
    val xs = key.split('.')
    (r: TableRow) => get(xs, 0, r)
  }

  def pathWithShards(path: String): String = path.replaceAll("\\/+$", "") + "/part"

  implicit class TextFileHeader(coll: SCollection[String]) {
    def saveAsTextFileWithHeader(path: String, header: String): Future[Tap[String]] = {
      val transform = TextIO.write()
        .to(pathWithShards(path))
        .withSuffix(".txt")
        .withNumShards(0)
        .withHeader(header)

      coll.saveAsCustomOutput("saveAsTextFileWithHeader", transform)
    }
  }

  /** Scio pipeline for BigDiffy. */
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val (mode, key, lhs, rhs, output, ignore, unordered) = {
      try {
        (args("mode"), args("key"), args("lhs"), args("rhs"), args("output"),
          args.list("ignore").toSet, args.list("unordered").toSet)
      } catch {
        case e: Throwable =>
          usage()
          throw e
      }
    }

    val result = mode match {
      case "avro" =>
        // TODO: handle schema evolution
        val fs = FileSystem.get(new URI(rhs), GcsConfiguration.get())
        val path = fs.globStatus(new Path(rhs)).head.getPath
        val schema = new AvroSampler(path).sample(1, true).head.getSchema
        val diffy = new AvroDiffy[GenericRecord](ignore, unordered)
        BigDiffy.diffAvro[GenericRecord](sc, lhs, rhs, avroKeyFn(key), diffy, schema)
      case "bigquery" =>
        // TODO: handle schema evolution
        val bq = BigQueryClient.defaultInstance()
        val lSchema = bq.getTableSchema(lhs)
        val rSchema = bq.getTableSchema(rhs)
        val schema = mergeTableSchema(lSchema, rSchema)
        val diffy = new TableRowDiffy(schema, ignore, unordered)
        BigDiffy.diffTableRow(sc, lhs, rhs, tableRowKeyFn(key), diffy)
      case m =>
        throw new IllegalArgumentException(s"mode $m not supported")
    }

    result.keyStats.map(_.toString).saveAsTextFileWithHeader(s"$output/keys",
        "key\tdifftype")

    result.fieldStats.map(_.toString).saveAsTextFileWithHeader(s"$output/fields",
        "field\tcount\tfraction\tdeltaStats")

    result.globalStats.map(_.toString).saveAsTextFileWithHeader(s"$output/global",
        "numTotal\tnumSame\tnumDiff\tnumMissingLhs\tnumMissingRhs")

    sc.close().waitUntilDone()
  }

}

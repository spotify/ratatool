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

import com.google.api.services.bigquery.model.{TableFieldSchema, TableRow, TableSchema}
import com.google.protobuf.AbstractMessage
import com.spotify.ratatool.BigQueryUtil.getFieldModeWithDefault
import com.spotify.ratatool.Command
import com.spotify.ratatool.io.ParquetIO
import com.spotify.ratatool.samplers.AvroSampler
import com.spotify.scio._
import com.spotify.scio.avro._
import com.spotify.scio.bigquery._
import com.spotify.scio.bigquery.client.BigQuery
import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.coders.Coder
import com.spotify.scio.coders.kryo._
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.parquet.avro._
import com.spotify.scio.values.SCollection
import com.twitter.algebird._
import org.apache.avro.{Schema, SchemaCompatibility, SchemaValidatorBuilder}
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecordBase
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.BaseEncoding
import org.slf4j.{Logger, LoggerFactory}

import java.nio.ByteBuffer
import java.util.Collections
import scala.annotation.tailrec
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.language.higherKinds
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

/**
 * Diff type between two records of the same key.
 *
 * SAME - the two records are identical. DIFFERENT - the two records are different. MISSING_LHS -
 * left hand side record is missing. MISSING_RHS - right hand side record is missing.
 */
object DiffType extends Enumeration {
  val SAME, DIFFERENT, MISSING_LHS, MISSING_RHS = Value
}

case class MultiKey(keys: Seq[String]) extends AnyVal {
  override def toString: String = keys.mkString("_")
}

object MultiKey {
  def apply(key: String): MultiKey = MultiKey(Seq(key))
}

/**
 * Key-field level [[DiffType]] and delta.
 *
 * If DiffType are SAME, MISSING_LHS, or MISSING_RHS they will appear once with no Delta If DiffType
 * is DIFFERENT, there is one KeyStats for every field that is different for that key with that
 * field's Delta
 *
 * keys - primary being compared. diffType - how the two records of the given key compares. delta -
 * a single field's difference including field name, values, and distance
 */
case class KeyStats(keys: MultiKey, diffType: DiffType.Value, delta: Option[Delta]) {
  override def toString: String = {
    val deltaStr = delta.map(_.toString).getOrElse("")
    s"$keys\t$diffType\t$deltaStr"
  }
}

/**
 * Global level statistics.
 *
 * numTotal - number of total unique keys. numSame - number of keys with same records on both sides.
 * numDiff - number of keys with different records on both sides. numMissingLhs - number of keys
 * with missing left hand side record. numMissingRhs - number of keys with missing right hand side
 * record.
 */
case class GlobalStats(
  numTotal: Long,
  numSame: Long,
  numDiff: Long,
  numMissingLhs: Long,
  numMissingRhs: Long
) {
  override def toString: String = s"$numTotal\t$numSame\t$numDiff\t$numMissingLhs\t$numMissingRhs"
}

/**
 * Delta level statistics, mean, and the four standardized moments.
 *
 * deltaType - one of NUMERIC, STRING, VECTOR min - minimum distance seen max - maximum distance
 * seen count - number of differences seen mean - mean of all differences variance - squared
 * deviation from the mean stddev - standard deviation from the mean skewness - measure of data
 * asymmetry in all deltas kurtosis - measure of distribution sharpness and tail thickness in deltas
 */
case class DeltaStats(
  deltaType: DeltaType.Value,
  min: Double,
  max: Double,
  count: Long,
  mean: Double,
  variance: Double,
  stddev: Double,
  skewness: Double,
  kurtosis: Double
) {
  override def toString: String =
    s"$deltaType\t$min\t$max\t$count\t$mean\t$variance\t$stddev\t$skewness\t$kurtosis"
}

/**
 * Field level statistics.
 *
 * field - "." separated field identifier. count - number of records with different values of the
 * given field. fraction - fraction over total number of keys with different records on both sides.
 * deltaStats - statistics of field value deltas.
 */
case class FieldStats(
  field: String,
  count: Long,
  fraction: Double,
  deltaStats: Option[DeltaStats]
) {
  override def toString: String =
    s"$field\t$count\t$fraction\t" + deltaStats.map(d => "\t" + d).getOrElse("")
}

/** Big diff between two data sets given a primary key. */
class BigDiffy[T: Coder](
  @transient val lhs: SCollection[T],
  @transient val rhs: SCollection[T],
  diffy: Diffy[T],
  keyFn: T => MultiKey,
  ignoreNan: Boolean = false
) extends Serializable {

  private lazy val _deltas: BigDiffy.DeltaSCollection =
    BigDiffy.computeDeltas(lhs, rhs, diffy, keyFn)

  private lazy val globalAndFieldStats: SCollection[(GlobalStats, Iterable[FieldStats])] =
    BigDiffy.computeGlobalAndFieldStats(_deltas, ignoreNan)

  /**
   * attempt to derive a Coder here will fail with divergent implicits so we fall back to kryo
   * serialization
   */
  implicit val deltasCoder: Coder[(MultiKey, String, Any, Any)] = Coder.kryo

  /**
   * Key and field level delta.
   *
   * Output tuples are (key, field, LHS, RHS). Note that LHS and RHS may not be serializable.
   */
  lazy val deltas: SCollection[(MultiKey, String, Any, Any)] =
    _deltas.flatMap { case (k, (ds, dt)) =>
      ds.map(d => (k, d.field, d.left, d.right))
    }

  /** Global level statistics. */
  lazy val globalStats: SCollection[GlobalStats] = globalAndFieldStats.keys

  /** Key level statistics. */
  lazy val keyStats: SCollection[KeyStats] = _deltas.flatMap { case (k, (dfs, dt)) =>
    dfs match {
      case Nil => Seq(KeyStats(k, dt, None))
      case _   => dfs.map(d => KeyStats(k, dt, Some(d)))
    }
  }

  /** Field level statistics. */
  lazy val fieldStats: SCollection[FieldStats] = globalAndFieldStats.flatMap(_._2)
}

sealed trait OutputMode
case object GCS extends OutputMode
case object BQ extends OutputMode

/** Big diff between two data sets given a primary key. */
object BigDiffy extends Command with Serializable {
  val command: String = "bigDiffy"
  @transient private lazy val logger: Logger = LoggerFactory.getLogger(BigDiffy.getClass)

  // (field, deltas, diff type)
  type DeltaSCollection = SCollection[(MultiKey, (Seq[Delta], DiffType.Value))]

  private def extractDeltaStats(
    tup: (DeltaType.Value, Min[Double], Max[Double], Moments)
  ): DeltaStats = {
    val (dt, min, max, m) = tup
    DeltaStats(
      deltaType = dt,
      min = min.get,
      max = max.get,
      count = m.count,
      mean = m.mean,
      variance = m.variance,
      stddev = m.stddev,
      skewness = m.skewness,
      kurtosis = m.kurtosis
    )
  }

  private def computeDeltas[T: Coder](
    lhs: SCollection[T],
    rhs: SCollection[T],
    diffy: Diffy[T],
    keyFn: T => MultiKey
  ): DeltaSCollection = {
    // extract keys and prefix records with L/R sub-key
    val lKeyed = lhs.map(t => (keyFn(t), ("l", t)))
    val rKeyed = rhs.map(t => (keyFn(t), ("r", t)))

    val sc = lhs.context
    val accSame = ScioMetrics.counter[Long]("SAME")
    val accDiff = ScioMetrics.counter[Long]("DIFFERENT")
    val accMissingLhs = ScioMetrics.counter[Long]("MISSING_LHS")
    val accMissingRhs = ScioMetrics.counter[Long]("MISSING_RHS")

    (lKeyed ++ rKeyed).groupByKey
      .map { case (key, values) => // values is a list of tuples: "l" -> record or "r" -> record
        if (values.size > 2) {
          throw new RuntimeException(s"""More than two values found for key: $key.
               | Your key must be unique in both SCollections""".stripMargin)
        }

        val valuesMap = values.toMap // L/R -> record
        if (valuesMap.size == 2) {
          val deltas: Seq[Delta] = diffy(valuesMap("l"), valuesMap("r"))
          val diffType = if (deltas.isEmpty) DiffType.SAME else DiffType.DIFFERENT
          (key, (deltas, diffType))
        } else {
          val diffType = if (valuesMap.contains("l")) DiffType.MISSING_RHS else DiffType.MISSING_LHS
          (key, (Nil, diffType))
        }
      }
      .tap {
        case (_, (_, DiffType.SAME))        => accSame.inc()
        case (_, (_, DiffType.DIFFERENT))   => accDiff.inc()
        case (_, (_, DiffType.MISSING_LHS)) => accMissingLhs.inc()
        case (_, (_, DiffType.MISSING_RHS)) => accMissingRhs.inc()
        case _                              =>
      }
  }

  private def computeGlobalAndFieldStats(
    deltas: DeltaSCollection,
    ignoreNan: Boolean
  ): SCollection[(GlobalStats, Iterable[FieldStats])] = {
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
            case UnknownDelta                             => None
            case TypedDelta(t, v) if ignoreNan && v.isNaN => None
            case TypedDelta(t, v) =>
              Some((t, Min(v), Max(v), Moments.aggregator.prepare(v)))
          }
          // Map of field -> (count, delta statistics)
          m(d.field) = (1L, optD)
        }
        // also sum global statistics
        val dtVec = dt match {
          case DiffType.SAME        => (1L, 1L, 0L, 0L, 0L)
          case DiffType.DIFFERENT   => (1L, 0L, 1L, 0L, 0L)
          case DiffType.MISSING_LHS => (1L, 0L, 0L, 1L, 0L)
          case DiffType.MISSING_RHS => (1L, 0L, 0L, 0L, 1L)
        }
        (dtVec, m.toMap)
      }
      .sum
      .map { case (dtVec, fieldMap) =>
        val globalKeyStats = GlobalStats.tupled(dtVec)
        val fieldStats = fieldMap.map { case (field, (count, optD)) =>
          val deltaStats = optD.map(extractDeltaStats _)
          val globalKeyStats = GlobalStats.tupled(dtVec)
          FieldStats(field, count, count.toDouble / globalKeyStats.numDiff, deltaStats)
        }
        (globalKeyStats, fieldStats)
      }
  }

  /** Diff two data sets. */
  def diff[T: ClassTag: Coder](
    lhs: SCollection[T],
    rhs: SCollection[T],
    d: Diffy[T],
    keyFn: T => MultiKey,
    ignoreNan: Boolean = false
  ): BigDiffy[T] =
    new BigDiffy[T](lhs, rhs, d, keyFn, ignoreNan)

  /** Diff two Avro data sets. */
  def diffAvro[T <: SpecificRecordBase: ClassTag: Coder](
    sc: ScioContext,
    lhs: String,
    rhs: String,
    keyFn: T => MultiKey,
    diffy: AvroDiffy[T],
    ignoreNan: Boolean = false
  ): BigDiffy[T] =
    diff(sc.avroFile[T](lhs), sc.avroFile[T](rhs), diffy, keyFn, ignoreNan)

  /** Diff two ProtoBuf data sets. */
  def diffProtoBuf[T <: AbstractMessage: ClassTag](
    sc: ScioContext,
    lhs: String,
    rhs: String,
    keyFn: T => MultiKey,
    diffy: ProtoBufDiffy[T]
  ): BigDiffy[T] =
    diff(sc.protobufFile(lhs), sc.protobufFile(rhs), diffy, keyFn)

  /**
   * Diff two Parquet data sets. Note that both typed-parquet and avro-parquet inputs are supported.
   * However, in either case the diff will be written in Parquet format as Avro GenericRecords.
   */
  def diffParquet(
    sc: ScioContext,
    lhs: String,
    rhs: String,
    keyFn: GenericRecord => MultiKey,
    diffy: AvroDiffy[GenericRecord]
  ): BigDiffy[GenericRecord] = {
    val compatSchema = ParquetIO.getCompatibleSchemaForFiles(lhs, rhs)
    implicit val grCoder: Coder[GenericRecord] = avroGenericRecordCoder(compatSchema)

    diff(
      sc.parquetAvroFile[GenericRecord](lhs, compatSchema).map(identity),
      sc.parquetAvroFile[GenericRecord](rhs, compatSchema).map(identity),
      diffy,
      keyFn
    )
  }

  /** Remove quotes wrapping string argument. * */
  def stripQuoteWrap(input: String): String = {
    val startChar = input.charAt(0)
    val endChar = input.charAt(input.length - 1)
    val quoteChars = List('"', '\'', '`')

    if (quoteChars.contains(startChar) && startChar == endChar) {
      input.slice(1, input.length - 1)
    } else {
      input
    }
  }

  /** Diff two TableRow data sets. */
  def diffTableRow(
    sc: ScioContext,
    lhs: String,
    rhs: String,
    rowRestriction: Option[String],
    keyFn: TableRow => MultiKey,
    diffy: TableRowDiffy,
    ignoreNan: Boolean = false
  ): BigDiffy[TableRow] = {
    // replace quotation marks at the beginning or end of the argument
    val restrictionCleaned = rowRestriction.map(stripQuoteWrap)

    diff(
      sc.bigQueryStorage(Table.Spec(lhs), rowRestriction = restrictionCleaned.orNull),
      sc.bigQueryStorage(Table.Spec(rhs), rowRestriction = restrictionCleaned.orNull),
      diffy,
      keyFn,
      ignoreNan
    )
  }

  /** Merge two BigQuery TableSchemas. */
  def mergeTableSchema(x: TableSchema, y: TableSchema): TableSchema = {
    val r = new TableSchema
    r.setFields(mergeFields(x.getFields.asScala.toList, y.getFields.asScala.toList).asJava)
  }

  @BigQueryType.toTable
  case class KeyStatsBigQuery(key: String, diffType: String, delta: Option[DeltaBigQuery])
  case class DeltaBigQuery(field: String, left: String, right: String, delta: DeltaValueBigQuery)
  case class DeltaValueBigQuery(deltaType: String, deltaValue: Option[Double])
  @BigQueryType.toTable
  case class GlobalStatsBigQuery(
    numTotal: Long,
    numSame: Long,
    numDiff: Long,
    numMissingLhs: Long,
    numMissingRhs: Long
  )
  @BigQueryType.toTable
  case class FieldStatsBigQuery(
    field: String,
    count: Long,
    fraction: Double,
    deltaStats: Option[DeltaStatsBigQuery]
  )
  case class DeltaStatsBigQuery(
    deltaType: String,
    min: Double,
    max: Double,
    count: Long,
    mean: Double,
    variance: Double,
    stddev: Double,
    skewness: Double,
    kurtosis: Double
  )

  /** saves stats to either GCS as text, or BigQuery */
  def saveStats[T](
    bigDiffy: BigDiffy[T],
    output: String,
    withHeader: Boolean = false,
    outputMode: OutputMode = GCS
  ): Unit = {
    outputMode match {
      case GCS =>
        // Saving to GCS, either with or without header
        val keyStatsPath = s"$output/keys"
        val fieldStatsPath = s"$output/fields"
        val globalStatsPath = s"$output/global"

        if (withHeader) {
          bigDiffy.keyStats
            .map(_.toString)
            .saveAsTextFileWithHeader(keyStatsPath, Seq("key", "difftype").mkString("\t"))
          bigDiffy.fieldStats
            .map(_.toString)
            .saveAsTextFileWithHeader(
              fieldStatsPath,
              Seq(
                "field",
                "count",
                "fraction",
                "deltaType",
                "min",
                "max",
                "count",
                "mean",
                "variance",
                "stddev",
                "skewness",
                "kurtosis"
              ).mkString("\t")
            )
          bigDiffy.globalStats
            .map(_.toString)
            .saveAsTextFileWithHeader(
              globalStatsPath,
              Seq("numTotal", "numSame", "numDiff", "numMissingLhs", "numMissingRhs").mkString("\t")
            )
        } else {
          bigDiffy.keyStats.saveAsTextFile(keyStatsPath)
          bigDiffy.fieldStats.saveAsTextFile(fieldStatsPath)
          bigDiffy.globalStats.saveAsTextFile(globalStatsPath)
        }
      case BQ =>
        // Saving to BQ, header irrelevant
        bigDiffy.keyStats
          .map(stat =>
            KeyStatsBigQuery(
              stat.keys.toString,
              stat.diffType.toString,
              stat.delta.map { d =>
                val dv = d.delta match {
                  case TypedDelta(dt, v) =>
                    DeltaValueBigQuery(dt.toString, Option(v))
                  case _ =>
                    DeltaValueBigQuery("UNKNOWN", None)
                }
                DeltaBigQuery(
                  d.field,
                  d.left.map(_.toString).getOrElse("null"),
                  d.right.map(_.toString).getOrElse("null"),
                  dv
                )
              }
            )
          )
          .saveAsTypedBigQueryTable(Table.Spec(s"${output}_keys"))
        bigDiffy.fieldStats
          .map(stat =>
            FieldStatsBigQuery(
              stat.field,
              stat.count,
              stat.fraction,
              stat.deltaStats.map(ds =>
                DeltaStatsBigQuery(
                  ds.deltaType.toString,
                  ds.min,
                  ds.max,
                  ds.count,
                  ds.mean,
                  ds.variance,
                  ds.stddev,
                  ds.skewness,
                  ds.kurtosis
                )
              )
            )
          )
          .saveAsTypedBigQueryTable(Table.Spec(s"${output}_fields"))
        bigDiffy.globalStats
          .map(stat =>
            GlobalStatsBigQuery(
              stat.numTotal,
              stat.numSame,
              stat.numDiff,
              stat.numMissingLhs,
              stat.numMissingRhs
            )
          )
          .saveAsTypedBigQueryTable(Table.Spec(s"${output}_global"))
    }
  }

  private def mergeFields(
    x: Seq[TableFieldSchema],
    y: Seq[TableFieldSchema]
  ): Seq[TableFieldSchema] = {
    val xMap = x.map(f => (f.getName, f)).toMap
    val yMap = y.map(f => (f.getName, f)).toMap
    val names = mutable.LinkedHashSet.empty[String]
    xMap.foreach(kv => names.add(kv._1))
    yMap.foreach(kv => names.add(kv._1))
    names.map { n =>
      (xMap.get(n), yMap.get(n)) match {
        case (Some(f), None) => f
        case (None, Some(f)) => f
        case (Some(fx), Some(fy)) =>
          val fxMode = getFieldModeWithDefault(fx.getMode)
          val fyMode = getFieldModeWithDefault(fy.getMode)
          assert(
            fx.getType == fy.getType && fxMode == fyMode,
            f"field ${fx.getName} in lhs, type: ${fx.getType} mode: $fxMode, and rhs, type: ${fy.getType} mode: $fyMode, do not match"
          )
          if (fx.getType == "RECORD") {
            fx.setFields(
              mergeFields(fx.getFields.asScala.toList, fy.getFields.asScala.toList).asJava
            )
          } else {
            fx
          }
        case _ => throw new RuntimeException
      }
    }.toSeq
  }

  private def usage(): Unit = {
    println(s"""BigDiffy - pair-wise field-level statistical diff
        |Usage: ratatool $command [dataflow_options] [options]
        |
        |  --input-mode=(avro|bigquery)     Diff-ing Avro or BQ records
        |  [--output-mode=(gcs|bigquery)]   Saves to a text file in GCS or a BigQuery dataset. Defaults to GCS
        |  --key=<key>                      '.' separated key field. Specify multiple --key params or multiple ',' separated key fields for multi key usage.
        |  --lhs=<path>                     LHS File path or BigQuery table
        |  --rhs=<path>                     RHS File path or BigQuery table
        |  [--rowRestriction=<filter>]      SQL text filtering statement to apply to BigQuery inputs (not available for avro inputs),
        |                                   similar to a WHERE clause in a query. Aggregates are not supported. Defaults to None
        |  --output=<output>                File path prefix for output
        |  --ignore=<keys>                  ',' separated field list to ignore
        |  --unordered=<keys>               ',' separated field list to treat as unordered
        |  --unorderedFieldKey=<key>        ',' separated list of keys for fields which are unordered nested records. Mappings use ':'
        |                                   For example --unorderedFieldKey=fieldPath:fieldKey,otherPath:otherKey
        |  [--with-header]                  Output all TSVs with header rows. Defaults to false
        |  [--ignore-nan]                   Ignore NaN values when computing stats for differences
        |
        |Since this runs a Scio/Beam pipeline, Dataflow options will have to be provided. At a
        |minimum, the following should be specified:
        |
        |   --project=<gcp-project-id>                GCP Project used to run your job
        |   --runner=DataflowRunner                   Executes the job on Google Cloud Dataflow
        |   --tempLocation=<gcs-path>                 Location for temporary files. GCS bucket must be created prior to running job.
        |
        |The following options are recommended, but may not be necessary.
        |
        |   --serviceAccount=<your-service-account>   Service account used on Dataflow workers. Useful to avoid permissions issues.
        |   --workerMachineType=<machine-type>        Can be tweaked based on your specific needs, but is not necessary.
        |   --maxNumWorkers=<num-workers>             Limits the number of workers (machines) used in the job to avoid using up quota.
        |
        |For more details regarding Dataflow options see here: https://cloud.google.com/dataflow/pipelines/specifying-exec-params
      """.stripMargin)
    sys.exit(1)
  }

  private def avroFileSchema(path: String, options: PipelineOptions): Schema =
    new AvroSampler(path, conf = Some(options)).sample(1, head = true).head.getSchema

  private[diffy] def avroKeyFn(keys: Seq[String]): GenericRecord => MultiKey = {
    @tailrec
    def get(xs: Array[String], i: Int, r: GenericRecord): String =
      if (i == xs.length - 1) {
        val valueOfKey = r.get(xs(i))
        if (valueOfKey == null) {
          logger.warn(s"""Null value found for key: ${xs.mkString(".")}.
               | If this is not expected check your data or use a different key.""".stripMargin)
        }
        // handle bytes keys custom, so we get bytebuffer actual content and not toString metadata
        if (valueOfKey.isInstanceOf[ByteBuffer]) {
          // encode to hex string
          BaseEncoding.base16().encode(valueOfKey.asInstanceOf[ByteBuffer].array())
        } else {
          String.valueOf(valueOfKey)
        }
      } else {
        get(xs, i + 1, r.get(xs(i)).asInstanceOf[GenericRecord])
      }

    val xs = keys.map(_.split('.'))
    (r: GenericRecord) => MultiKey(xs.map(x => get(x, 0, r)))
  }

  private[diffy] def tableRowKeyFn(keys: Seq[String]): TableRow => MultiKey = {
    @tailrec
    def get(xs: Array[String], i: Int, r: java.util.Map[String, AnyRef]): String =
      if (i == xs.length - 1) {
        val valueOfKey = r.get(xs(i))
        if (valueOfKey == null) {
          logger.warn(s"""Null value found for key: ${xs.mkString(".")}.
               | If this is not expected check your data or use a different key.""".stripMargin)
        }

        // Implicitly converts nulls to "null"
        // Same as for Avro string case above
        String.valueOf(valueOfKey)
      } else {
        get(xs, i + 1, r.get(xs(i)).asInstanceOf[java.util.Map[String, AnyRef]])
      }

    val xs = keys.map(_.split('.'))
    (r: TableRow) => MultiKey(xs.map(x => get(x, 0, r)))
  }

  private[diffy] def unorderedKeysMap(unorderedKeysArgs: List[String]): Try[Map[String, String]] = {
    Try(unorderedKeysArgs.map { arg =>
      val keyMappings = arg.split(":")
      assert(keyMappings.size == 2, s"Invalid unordered field key mapping $arg")
      (keyMappings(0), keyMappings(1))
    }.toMap)
  }

  def pathWithShards(path: String): String = path.replaceAll("\\/+$", "") + "/part"

  implicit class TextFileHeader(coll: SCollection[String]) {
    def saveAsTextFileWithHeader(path: String, header: String): ClosedTap[Nothing] = {
      val transform = TextIO
        .write()
        .to(pathWithShards(path))
        .withSuffix(".txt")
        .withNumShards(0)
        .withHeader(header)

      coll.saveAsCustomOutput("saveAsTextFileWithHeader", transform)
    }
  }

  /** for easier running via sbt */
  def main(cmdlineArgs: Array[String]): Unit = run(cmdlineArgs)

  /** Scio pipeline for BigDiffy. */
  def run(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val (
      inputMode,
      keys,
      lhs,
      rhs,
      rowRestriction,
      output,
      header,
      ignore,
      unordered,
      unorderedKeysList,
      outputMode,
      ignoreNan
    ) = {
      try {
        (
          args("input-mode"),
          args.list("key"),
          args("lhs"),
          args("rhs"),
          args.optional("rowRestriction"),
          args("output"),
          args.boolean("with-header", false),
          args.list("ignore").toSet,
          args.list("unordered").toSet,
          args.list("unorderedFieldKey"),
          args.optional("output-mode"),
          args.boolean("ignore-nan", false)
        )
      } catch {
        case e: Throwable =>
          usage()
          throw e
      }
    }

    val unorderedKeys = unorderedKeysMap(unorderedKeysList) match {
      case Success(m) => m
      case Failure(e) =>
        usage()
        throw e
    }

    val om: OutputMode = outputMode match {
      case Some("gcs")      => GCS
      case Some("bigquery") => BQ
      case None             => GCS
      case m                => throw new IllegalArgumentException(s"output mode $m not supported")
    }

    if (om == GCS && !output.startsWith("gs://")) {
      // if combo of inputs is invalid, error out early
      throw new IllegalArgumentException(
        s"Output mode is GCS, " +
          s"but output $output is not a valid GCS location"
      )
    }

    // validity checks passed, ok to run the diff
    val result = inputMode match {
      case "avro" =>
        if (rowRestriction.isDefined) {
          throw new IllegalArgumentException("rowRestriction cannot be passed for avro inputs")
        }

        val lhsSchema = avroFileSchema(lhs, sc.options)
        val rhsSchema = avroFileSchema(rhs, sc.options)

        // validate the rhs schema can be used to read lhs
        new SchemaValidatorBuilder().canReadStrategy
          .validateLatest()
          .validate(rhsSchema, Collections.singletonList(lhsSchema))

        if (lhsSchema != rhsSchema) {
          logger.warn("Schemas are different but compatible, using the rhs schema for diff")
        }
        val schema = rhsSchema

        implicit val grCoder: Coder[GenericRecord] = avroGenericRecordCoder(schema)
        val diffy = new AvroDiffy[GenericRecord](ignore, unordered, unorderedKeys)
        val lhsSCollection = sc.avroFile(lhs, schema)
        val rhsSCollection = sc.avroFile(rhs, schema)
        BigDiffy
          .diff[GenericRecord](lhsSCollection, rhsSCollection, diffy, avroKeyFn(keys), ignoreNan)
      case "parquet" =>
        if (rowRestriction.isDefined) {
          throw new IllegalArgumentException("rowRestriction cannot be passed for Parquet inputs")
        }
        val compatSchema = ParquetIO.getCompatibleSchemaForFiles(lhs, rhs)
        val diffy = new AvroDiffy[GenericRecord](ignore, unordered, unorderedKeys)(
          avroGenericRecordCoder(compatSchema)
        )
        BigDiffy.diffParquet(sc, lhs, rhs, avroKeyFn(keys), diffy)
      case "bigquery" =>
        // TODO: handle schema evolution
        val bq = BigQuery.defaultInstance()
        val lSchema = bq.tables.schema(lhs)
        val rSchema = bq.tables.schema(rhs)
        val schema = mergeTableSchema(lSchema, rSchema)
        val diffy = new TableRowDiffy(schema, ignore, unordered, unorderedKeys)
        BigDiffy.diffTableRow(sc, lhs, rhs, rowRestriction, tableRowKeyFn(keys), diffy, ignoreNan)
      case m =>
        throw new IllegalArgumentException(s"input mode $m not supported")
    }
    saveStats(result, output, header, om)

    sc.run().waitUntilDone()
  }
}

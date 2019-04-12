/*
 * Copyright 2017 Spotify AB.
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
package com.spotify.ratatool.samplers

import java.net.URI
import java.nio.charset.Charset

import com.google.api.services.bigquery.model.{TableFieldSchema, TableReference}
import com.google.common.hash.{HashCode, Hasher, Hashing}
import com.spotify.ratatool.samplers.util.SamplerSCollectionFunctions._
import com.spotify.ratatool.Command
import com.spotify.ratatool.avro.specific.TestRecord
import com.spotify.ratatool.samplers.util._
import com.spotify.scio.bigquery.TableRow
import com.spotify.scio.io.Tap
import com.spotify.scio.values.SCollection
import com.spotify.scio.{ContextAndArgs, ScioContext}
import com.spotify.scio.coders.Coder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers
import org.apache.beam.sdk.options.PipelineOptions
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.language.existentials
import scala.util.Try
import scala.reflect.ClassTag

object BigSampler extends Command {
  val command: String = "bigSampler"

  private val log = LoggerFactory.getLogger(BigSampler.getClass)
  private[samplers] val utf8Charset = Charset.forName("UTF-8")
  private[samplers] val fieldSep = '.'

  private[samplers] sealed trait HashAlgorithm {
    def hashFn(seed: Option[Int]): Hasher
  }
  private[samplers] case object MurmurHash extends HashAlgorithm {
    def hashFn(seed:Option[Int]): Hasher = {
      Hashing.murmur3_128(seed.getOrElse(System.currentTimeMillis().toInt)).newHasher()
    }
  }
  private[samplers] case object FarmHash extends HashAlgorithm {
    def hashFn(seed: Option[Int]): Hasher = seed match {
      case Some(s) => Hashing.farmHashFingerprint64().newHasher().putInt(s)
      case _ => Hashing.farmHashFingerprint64().newHasher()
    }
  }

  /**
    * @param hashAlgorithm either MurmurHash (for backwards compatibility) or FarmHash
    * @param seed optional start value to ensure the same result every time if same seed passed in
    * @return a hasher to use when sampling
    */
  private[samplers] def hashFun(hashAlgorithm: HashAlgorithm = FarmHash,
                                seed: Option[Int] = None): Hasher = {
    hashAlgorithm.hashFn(seed)
  }

  /**
    * Maps a long value to a double in [0, 1] such that Long.MinValue -> 0.0 and
    * Long.MaxValue -> 1.0.
    *
    * @param a A long value.
    * @return The [0, 1] bounded double.
    */
  private[samplers] def boundLong(a: Long): Double = {
    (a.toDouble - Long.MinValue.toDouble) / (Long.MaxValue.toDouble - Long.MinValue.toDouble)
  }

  /**
   * Internal element dicing method.
   *
   * @param sampleFraction (0.0, 1.0]
   */
  private[samplers] def diceElement[T](e: T, hash: HashCode, sampleFraction: Double): Option[T] = {
    //TODO: for now leave it up to jit/compiler to optimize
    if (boundLong(hash.asLong) < sampleFraction) {
      Some(e)
    } else {
      None
    }
  }

  private def parseAsBigQueryTable(tblRef: String): Option[TableReference] = {
    Try(BigQueryHelpers.parseTableSpec(tblRef)).toOption
  }

  private def parseAsURI(uri: String): Option[URI] = {
    Try(new URI(uri)).toOption
  }

  private def usage(): Unit = {
    // scalastyle:off regex line.size.limit
    // TODO: Rename --exact to something better
    println(
      s"""BigSampler - a tool for big data sampling
        |Usage: ratatool $command [dataflow_options] [options]
        |
        |  --sample=<percentage>                      Percentage of records to take in sample, a decimal between 0.0 and 1.0
        |  --input=<path>                             Input file path or BigQuery table
        |  --output=<path>                            Output file path or BigQuery table
        |  [--fields=<field1,field2,...>]             An optional list of fields to include in hashing for sampling cohort selection
        |  [--seed=<seed>]                            An optional seed used in hashing for sampling cohort selection
        |  [--distribution=(uniform|stratified)]      An optional arg to sample for a stratified or uniform distribution. Must provide `distributionFields`
        |  [--distributionFields=<field1,field2,...>] An optional list of fields to sample for distribution. Must provide `distribution`
        |  [--exact]                                  An optional arg for higher precision distribution sampling.
        |  [--byteEncoding=(raw|hex|base64)]          An optional arg for how to encode fields of type bytes: raw bytes, hex encoded string, or base64 encoded string. Default is to hash raw bytes.
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
    // scalastyle:on regex line.size.limit
    sys.exit(1)
  }


  private[samplers] def hashTableRow(r: TableRow,
                                     f: String,
                                     tblSchemaFields: Seq[TableFieldSchema],
                                     hasher: Hasher): Hasher =
    BigSamplerBigQuery.hashTableRow(tblSchemaFields)(r, f, hasher)

  private[samplers] def hashAvroField(r: TestRecord,
                                      f: String,
                                      avroSchema: Schema,
                                      hasher: Hasher): Hasher =
    BigSamplerAvro.hashAvroField(avroSchema)(r, f, hasher)

  private[samplers] def hashAvroField(r: GenericRecord,
                                      f: String,
                                      avroSchema: Schema,
                                      hasher: Hasher): Hasher =
    BigSamplerAvro.hashAvroField(avroSchema)(r, f, hasher)

  //scalastyle:off method.length cyclomatic.complexity
  def singleInput(argv: Array[String]): Future[Tap[_]] = {
    val (sc, args) = ContextAndArgs(argv)
    val (opts, _) = ScioContext.parseArguments[PipelineOptions](argv)
    // Determines how large our heap should be for topByKey
    val sizePerKey =
      if (Try(opts.asInstanceOf[DataflowPipelineWorkerPoolOptions].getWorkerMachineType)
          .map(_.split("-").last.toInt).getOrElse(4) > 8){
        1e9.toInt
      } else {
        1e6.toInt
      }

    val (samplePct, input, output, fields, seed, distribution, distributionFields, exact) = try {
      val pct = args("sample").toFloat
      require(pct > 0.0F && pct <= 1.0F)
      (pct,
        args("input"),
        args("output"),
        args.list("fields"),
        args.optional("seed"),
        args.optional("distribution").map(SampleDistribution.fromString),
        args.list("distributionFields"),
        Precision.fromBoolean(args.boolean("exact", default = false)))
    } catch {
      case e: Throwable =>
        usage()
        throw e
    }

    val byteEncoding = ByteEncoding.fromString(args.getOrElse("byteEncoding", "raw"))

    if (fields.isEmpty) {
      log.warn("No fields to hash on specified, won't guarantee cohorts between datasets.")
    }

    if (seed.isEmpty) {
      log.warn("No seed specified, won't guarantee cohorts between datasets.")
    }

    if (distribution.isEmpty) {
      log.warn("No distribution specified, won't guarantee output distribution")
    }

    if (distribution.isDefined && distributionFields.isEmpty) {
      throw new IllegalArgumentException(
        "distributionFields must be specified if a distribution is given")
    }

    if (parseAsBigQueryTable(input).isDefined) {
      require(parseAsBigQueryTable(output).isDefined,
        s"Input is a BigQuery table `$input`, output should be a BigQuery table too," +
          s"but instead it's `$output`.")
      val inputTbl = parseAsBigQueryTable(input).get
      val outputTbl = parseAsBigQueryTable(output).get
      BigSamplerBigQuery.sample(
        sc,
        inputTbl,
        outputTbl,
        fields,
        samplePct,
        seed.map(_.toInt),
        distribution,
        distributionFields,
        exact,
        sizePerKey,
        byteEncoding
      )
    } else if (parseAsURI(input).isDefined) {
      // right now only support for avro
      require(parseAsURI(output).isDefined,
        s"Input is a URI: `$input`, output should be a URI too, but instead it's `$output`.")
      // Prompts FileSystems to load service classes, otherwise fetching schema from non-local fails
      FileSystems.setDefaultPipelineOptions(opts)
      BigSamplerAvro.sample(
        sc,
        input,
        output,
        fields,
        samplePct,
        seed.map(_.toInt),
        distribution,
        distributionFields,
        exact,
        sizePerKey,
        byteEncoding
      )
    } else {
      throw new UnsupportedOperationException(s"Input `$input not supported.")
    }
  }
  //scalastyle:on method.length cyclomatic.complexity

  //scalastyle:off method.length cyclomatic.complexity parameter.number
  /**
   * Sample wrapper function that manages sampling pipeline based on determinimism, precision,
   *  and data type. Can be used to build sampling for data types not supported out of the box.
   * @param s The input SCollection to be sampled
   * @param fraction The sample rate
   * @param fields Fields to construct hash over for determinism
   * @param seed Seed used to salt the deterministic hash
   * @param distribution Desired output sample distribution
   * @param distributionFields Fields to construct distribution over (strata = set of unique fields)
   * @param precision Approximate or Exact precision
   * @param hashFn Function to construct a hash given a record, field, and hasher
   * @param keyFn Function to extract a value that's safe to serialize and key on, given a record
   * @param maxKeySize Maximum allowed size per key (can be tweaked for very large data sets)
   * @param byteEncoding Determines how bytes are encoded prior to hashing.
   * @tparam T Record Type
   * @tparam U Key Type, usually we use Set[String]
   * @return SCollection containing Sample population
   */
  def sample[T: ClassTag : Coder, U: ClassTag : Coder](s: SCollection[T],
                                        fraction: Double,
                                        fields: Seq[String],
                                        seed: Option[Int],
                                        distribution: Option[SampleDistribution],
                                        distributionFields: Seq[String],
                                        precision: Precision,
                                        hashFn: (T, String, Hasher) => Hasher,
                                        keyFn: T => U,
                                        maxKeySize: Int = 1e6.toInt,
                                        byteEncoding: ByteEncoding = RawEncoding)
  : SCollection[T] = {
    def assignHashRoll(s: SCollection[T],
                       seed: Option[Int],
                       fields: Seq[String])
    : SCollection[(U, (T, Double))] = {
      s.map { v =>
        val hasher =
          ByteHasher.wrap(BigSampler.hashFun(seed = seed), byteEncoding, utf8Charset)
        val hash = fields.foldLeft(hasher)((h, f) => hashFn(v, f, h)).hash
        (keyFn(v), (v, boundLong(hash.asLong)))
      }
    }

    @transient lazy val logSerDe = LoggerFactory.getLogger(this.getClass)
    val det = Determinism.fromSeq(fields)

    (det, distribution, precision) match {
      case (NonDeterministic, None, Approximate) => s.sample(withReplacement = false, fraction)

      case (NonDeterministic, Some(d), Approximate) =>
        s.sampleDist(d, keyFn, fraction)

      case (Deterministic, None, Approximate) =>
        s.flatMap { e =>
          val hasher =
            ByteHasher.wrap(BigSampler.hashFun(seed = seed), byteEncoding, utf8Charset)
          val hash = fields.foldLeft(hasher)((h, f) => hashFn(e, f, h)).hash
          BigSampler.diceElement(e, hash, fraction)
        }

      case (Deterministic, Some(StratifiedDistribution), Approximate) =>
        val sampled = s.flatMap { v =>
          val hasher =
            ByteHasher.wrap(BigSampler.hashFun(seed = seed), byteEncoding, utf8Charset)
          val hash = fields.foldLeft(hasher)((h, f) => hashFn(v, f, h)).hash
          BigSampler.diceElement(v, hash, fraction)
        }.keyBy(keyFn(_))

        val sampledDiffs = buildStratifiedDiffs(s, sampled, keyFn, fraction)
        logDistributionDiffs(sampledDiffs, logSerDe)
        sampled.values

      case (Deterministic, Some(UniformDistribution), Approximate) =>
        val (popPerKey, probPerKey) = uniformParams(s, keyFn, fraction)
        val sampled = s.keyBy(keyFn(_))
          .hashJoin(probPerKey).flatMap { case (k, (v, prob)) =>
          val hasher =
            ByteHasher.wrap(BigSampler.hashFun(seed = seed), byteEncoding, utf8Charset)
          val hash = fields.foldLeft(hasher)((h, f) => hashFn(v, f, h)).hash
          BigSampler.diceElement(v, hash, prob).map(e => (k, e))
        }

        val sampledDiffs =
          buildUniformDiffs(s, sampled, keyFn, fraction, popPerKey)
        logDistributionDiffs(sampledDiffs, logSerDe)
        sampled.values

      case (NonDeterministic, Some(d), Exact) =>
        assignRandomRoll(s, keyFn)
          .exactSampleDist(d, keyFn, fraction, maxKeySize)

      case (Deterministic, Some(d), Exact) =>
        assignHashRoll(s, seed, fields)
          .exactSampleDist(d, keyFn, fraction, maxKeySize, delta = 1e-6)

      case _ =>
        throw new UnsupportedOperationException("This sampling mode is not currently supported")
    }
  }
  //scalastyle:on method.length cyclomatic.complexity

  def run(argv: Array[String]): Unit = {
    this.singleInput(argv)
  }
}



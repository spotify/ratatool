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

package com.spotify.ratatool

import com.google.api.services.bigquery.model.TableSchema
import com.google.protobuf.AbstractMessage
import com.spotify.ratatool.samplers.util.{Approximate, Precision, SampleDistribution}
import com.spotify.ratatool.serde.JsonSerDe
import com.spotify.scio.bigquery.TableRow
import com.spotify.scio.values.SCollection
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

package object samplers {
  //scalastyle:off parameter.number
  /**
   * Sample wrapper function for Avro GenericRecord
   * @param coll The input SCollection to be sampled
   * @param fraction The sample rate
   * @param fields Fields to construct hash over for determinism
   * @param seed Seed used to salt the deterministic hash
   * @param distribution Desired output sample distribution
   * @param distributionFields Fields to construct distribution over (strata = set of unique fields)
   * @param precision Approximate or Exact precision
   * @param maxKeySize Maximum allowed size per key (can be tweaked for very large data sets)
   * @tparam T Record Type
   * @return SCollection containing Sample population
   */
  def sampleAvro[T <: GenericRecord : ClassTag](coll: SCollection[T],
                                                 fraction: Double,
                                                 schema: => Schema,
                                                 fields: Seq[String] = Seq(),
                                                 seed: Option[Int] = None,
                                                 distribution: Option[SampleDistribution] = None,
                                                 distributionFields: Seq[String] = Seq(),
                                                 precision: Precision = Approximate,
                                                 maxKeySize: Int = 1e6.toInt)
  : SCollection[T] = {
    val schemaSer = schema.toString(false)
    @transient lazy val schemaSerDe = new Schema.Parser().parse(schemaSer)

    BigSampler.sample(coll, fraction, fields, seed, distribution, distributionFields, precision,
      BigSamplerAvro.hashAvroField(schemaSerDe),
      BigSamplerAvro.buildKey(schemaSerDe, distributionFields),
      maxKeySize)
  }

  /**
   * Sample wrapper function for Avro GenericRecord
   * @param coll The input SCollection to be sampled
   * @param fraction The sample rate
   * @param fields Fields to construct hash over for determinism
   * @param seed Seed used to salt the deterministic hash
   * @param distribution Desired output sample distribution
   * @param distributionFields Fields to construct distribution over (strata = set of unique fields)
   * @param precision Approximate or Exact precision
   * @param maxKeySize Maximum allowed size per key (can be tweaked for very large data sets)
   * @return SCollection containing Sample population
   */
  def sampleTableRow(coll: SCollection[TableRow],
                     fraction: Double,
                     schema: TableSchema,
                     fields: Seq[String] = Seq(),
                     seed: Option[Int] = None,
                     distribution: Option[SampleDistribution] = None,
                     distributionFields: Seq[String] = Seq(),
                     precision: Precision = Approximate,
                     maxKeySize: Int = 1e6.toInt)
  : SCollection[TableRow] = {
    val schemaStr = JsonSerDe.toJsonString(schema)
    @transient lazy val schemaFields =
      JsonSerDe.fromJsonString(schemaStr, classOf[TableSchema]).getFields.asScala

    BigSampler.sample(coll, fraction, fields, seed, distribution, distributionFields, precision,
      BigSamplerBigQuery.hashTableRow(schemaFields),
      BigSamplerBigQuery.buildKey(schemaFields, distributionFields), maxKeySize)
  }

  /**
   * Sample wrapper function for Avro GenericRecord
   * @param coll The input SCollection to be sampled
   * @param fraction The sample rate
   * @param fields Fields to construct hash over for determinism
   * @param seed Seed used to salt the deterministic hash
   * @param distribution Desired output sample distribution
   * @param distributionFields Fields to construct distribution over (strata = set of unique fields)
   * @param precision Approximate or Exact precision
   * @param maxKeySize Maximum allowed size per key (can be tweaked for very large data sets)
   * @tparam T Record Type
   * @return SCollection containing Sample population
   */
  def sampleProto[T <: AbstractMessage : ClassTag](coll: SCollection[T],
                                                   fraction: Double,
                                                   fields: Seq[String] = Seq(),
                                                   seed: Option[Int] = None,
                                                   distribution: Option[SampleDistribution]=None,
                                                   distributionFields: Seq[String] = Seq(),
                                                   precision: Precision = Approximate,
                                                   maxKeySize: Int = 1e6.toInt)
  : SCollection[T] = {
    BigSampler.sample(coll, fraction, fields, seed, distribution, distributionFields, precision,
      BigSamplerProtobuf.hashProtobufField, BigSamplerProtobuf.buildKey(distributionFields),
      maxKeySize)
  }
  //scalastyle:on parameter.number

}

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
  implicit class BigSamplerAvroFn[T <: GenericRecord : ClassTag](coll: SCollection[T]) {
    def sampleAvro(fraction: Double,
                   schema: Schema,
                   fields: Seq[String],
                   seed: Option[Int] = None,
                   distribution: Option[SampleDistribution] = None,
                   distributionFields: Seq[String] = Seq(),
                   precision: Precision = Approximate,
                   maxKeySize: Int = 1e6.toInt): SCollection[T] = {
      val schemaSer = schema.toString(false)
      @transient lazy val schemaSerDe = new Schema.Parser().parse(schemaSer)
      BigSampler.sample(coll, fields, fraction, seed, distribution, distributionFields, precision,
        BigSamplerAvro.hashAvroField(schemaSerDe),
        BigSamplerAvro.buildKey(schemaSerDe, distributionFields),
        maxKeySize)
    }
  }

  implicit class BigSamplerTableRowFn(coll: SCollection[TableRow]) {
    def sampleTableRow(fraction: Double,
                       schema: TableSchema,
                       fields: Seq[String] = Seq(),
                       seed: Option[Int] = None,
                       distribution: Option[SampleDistribution] = None,
                       distributionFields: Seq[String] = Seq(),
                       precision: Precision = Approximate,
                       maxKeySize: Int = 1e8.toInt): SCollection[TableRow] = {
      val schemaStr = JsonSerDe.toJsonString(schema)

      @transient lazy val schemaFields =
        JsonSerDe.fromJsonString(schemaStr, classOf[TableSchema]).getFields.asScala

      BigSampler.sample(coll, fields, fraction, seed, distribution, distributionFields, precision,
        BigSamplerBigQuery.hashTableRow(schemaFields),
        BigSamplerBigQuery.buildKey(schemaFields, distributionFields), maxKeySize)
    }
  }

  implicit class BigSamplerProtoFn[T <: AbstractMessage : ClassTag](coll: SCollection[T]) {
    def sampleProto(fraction: Double,
                   fields: Seq[String],
                   seed: Option[Int] = None,
                   distribution: Option[SampleDistribution] = None,
                   distributionFields: Seq[String] = Seq(),
                   precision: Precision = Approximate,
                   maxKeySize: Int = 1e6.toInt): SCollection[T] = {
      BigSampler.sample(coll, fields, fraction, seed, distribution, distributionFields, precision,
        BigSamplerProtobuf.hashProtobufField, BigSamplerProtobuf.buildKey(distributionFields),
        maxKeySize)
    }
  }
}

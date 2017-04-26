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

import com.spotify.ratatool.Schemas
import com.spotify.ratatool.avro.specific.TestRecord
import com.spotify.ratatool.scalacheck.{AvroGen, TableRowGen}
import org.apache.avro.generic.GenericRecord
import org.scalacheck.Prop.{all, forAll, proved}
import org.scalacheck.{Gen, Properties}

import scala.collection.JavaConverters._

object BigSamplerTest extends Properties("BigSampler") {

  private val testSeed = Option(42)
  private def newTestHasher(seed: Option[Int] = testSeed) = BigSampler.kissHashFun(seed).newHasher()

  property("dice on the same element should match") = forAll { i: Int =>
    val hasher1 = newTestHasher()
    hasher1.putInt(i)
    val dice1 = BigSampler.diceElement(i, hasher1.hash(), 1, 10)
    val hasher2 = newTestHasher()
    hasher2.putInt(i)
    val dice2 = BigSampler.diceElement(i, hasher2.hash(), 1, 10)
    dice1 == dice2
  }

  property("kiss hasher should returns different hash on different seed") = forAll { i: Int =>
    val hasher1 = newTestHasher()
    val hasher2 = newTestHasher(testSeed.map(_ + 1))
    hasher1.putInt(i).hash() != hasher2.putInt(i).hash()
  }

  import TableRowGen._
  private val tblSchemaFields = Schemas.tableSchema.getFields.asScala
  private val richTableRowGen = tableRowOf(Schemas.tableSchema)
  private val supportedTableRowTypes = Seq(
    "int_field",
    "float_field",
    "boolean_field",
    "string_field",
    "timestamp_field",
    "date_field",
    "time_field",
    "datetime_field")

  property("should hash on supported types in TableRow") = forAll(richTableRowGen) { r =>
    val hasher = newTestHasher()
    all(
      supportedTableRowTypes.map(t => s"required_fields.$t").map( f =>
        {
          BigSampler.hashTableRow(r,
            f,
            tblSchemaFields,
            hasher)
          proved} :| f
      ): _*)
  }

  property("should hash on supported nullable types in TableRow") = forAll(richTableRowGen) { r =>
    val hasher = newTestHasher()
    all(
      supportedTableRowTypes.map(t => s"nullable_fields.$t").map( f =>
        {
          BigSampler.hashTableRow(r,
            f,
            tblSchemaFields,
            hasher)
          proved} :| f
      ): _*)
  }

  property("should hash on supported repeated types in TableRow") = forAll(richTableRowGen) { r =>
    val hasher = newTestHasher()
    all(
      supportedTableRowTypes.map(t => s"repeated_fields.$t").map( f =>
        {
          BigSampler.hashTableRow(r,
            f,
            tblSchemaFields,
            hasher)
          proved} :| f
      ): _*)
  }

  import AvroGen._
  private val avroSchema = TestRecord.SCHEMA$
  private val richAvroGen = avroOf[TestRecord]
  private val supportedAvroTypes = Seq(
    "int_field",
    "float_field",
    "boolean_field",
    "string_field",
    "long_field",
    "double_field")

  property("should hash on supported required types in Avro") = forAll(richAvroGen) { r =>
    val hasher = newTestHasher()
    all(
      supportedAvroTypes.map(t => s"required_fields.$t").map( f =>
        {
          BigSampler.hashAvroField(r,
            f,
            avroSchema,
            hasher)
          proved} :| f
      ): _*)
  }

  property("should hash on supported nullable types in Avro") = forAll(richAvroGen) { r =>
    val hasher = newTestHasher()
    all(
      supportedAvroTypes.map(t => s"nullable_fields.$t").map( f =>
        {
          BigSampler.hashAvroField(r,
            f,
            avroSchema,
            hasher)
            proved} :| f
      ): _*)
  }

  property("should hash on supported repeated types in Avro") = forAll(richAvroGen) { r =>
    val hasher = newTestHasher()
    all(
      supportedAvroTypes.map(t => s"repeated_fields.$t").map( f =>
        {
          BigSampler.hashAvroField(r,
            f,
            avroSchema,
            hasher)
          proved} :| f
      ): _*)
  }

  private val supportedCommonFields = Seq(
    "int_field",
    "float_field",
    "boolean_field",
    "string_field")
  private val records = Seq("nullable_fields", "repeated_fields", "required_fields")

  property("hash of the same single field should match") = forAll(
    Gen.zip(richAvroGen, Gen.oneOf(supportedCommonFields), Gen.oneOf(records)).map {
      case (i, f, r) =>
        val tbGen = richTableRowGen.amend(
          Gen.const(i.get(r).asInstanceOf[GenericRecord].get(f)))(_.getRecord(r).set(f))
    (i, tbGen.sample.get, s"$r.$f")
  }) { case (avro, tblRow, f) =>
    val tblHash = BigSampler.hashTableRow(tblRow, f, tblSchemaFields, newTestHasher()).hash()
    val avroHash = BigSampler.hashAvroField(avro, f, avroSchema, newTestHasher()).hash()
    tblHash == avroHash
  }

  property("hash of the same fields from the same record should match") = forAll(
    Gen.zip(richAvroGen, Gen.someOf(supportedCommonFields), Gen.oneOf(records)).map {
      case (i, fs, r) =>
        val tbGen = fs.foldLeft(richTableRowGen)((gen, f) => gen.amend(
          Gen.const(i.get(r).asInstanceOf[GenericRecord].get(f)))(_.getRecord(r).set(f)))
        (i, tbGen.sample.get, fs.map(f => s"$r.$f"))
    }) { case (avro, tblRow, fs) =>
    val hashes = fs.foldLeft((newTestHasher(), newTestHasher())){ case ((h1, h2), f) =>
      (BigSampler.hashTableRow(tblRow, f, tblSchemaFields, h1),
        BigSampler.hashAvroField(avro, f, avroSchema, h2))
    }
    hashes._1.hash() == hashes._2.hash()
  }

  property("hash of the same fields from multiple record should match") = forAll(
    Gen.zip(
      richAvroGen,
      Gen.someOf(supportedCommonFields).suchThat(_.nonEmpty),
      Gen.someOf(records).suchThat(_.nonEmpty)).map {
      case (i, fs, rs) =>
        val crossFieldToRecord = for {f <- fs; r <- rs} yield (f,r)
        val tbGen = crossFieldToRecord.foldLeft(richTableRowGen){ case (gen, (f, r)) =>
          gen.amend(Gen.const(i.get(r).asInstanceOf[GenericRecord].get(f)))(_.getRecord(r).set(f))}
        (i, tbGen.sample.get, crossFieldToRecord.map{ case (f,r) => s"$r.$f" })
    }) { case (avro, tblRow, fields) =>
    val hashes = fields.foldLeft((newTestHasher(), newTestHasher())){ case ((h1, h2), f) =>
      (BigSampler.hashTableRow(tblRow, f, tblSchemaFields, h1),
        BigSampler.hashAvroField(avro, f, avroSchema, h2))
    }
    hashes._1.hash() == hashes._2.hash()
  }

}

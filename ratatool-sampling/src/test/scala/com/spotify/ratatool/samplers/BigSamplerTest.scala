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

import java.io.File
import java.nio.file.Files

import com.google.common.hash.HashCode
import com.spotify.ratatool.Schemas
import com.spotify.ratatool.avro.specific.TestRecord
import com.spotify.ratatool.scalacheck._
import com.spotify.ratatool.io.{AvroIO, FileStorage}
import org.apache.avro.generic.GenericRecord
import org.scalacheck.Prop.{all, forAll, proved}
import org.scalacheck.rng.Seed
import org.scalacheck.{Gen, Properties}
import org.scalatest.{BeforeAndAfterAllConfigMap, ConfigMap, FlatSpec, Matchers}

import scala.collection.JavaConverters._

object BigSamplerTest extends Properties("BigSampler") {

  private val testSeed = Some(42)
  private def newTestFarmHasher() = BigSampler.hashFun().newHasher()
  private def newTestKissHasher(testSeed: Option[Int] = testSeed) =
    BigSampler.hashFun(hashAlgorithm = BigSampler.MurmurHash, murmurSeed = testSeed).newHasher()

  property("dice on the same element should match") = forAll { i: Int =>
    val hasher1 = newTestFarmHasher()
    hasher1.putInt(i)
    val dice1 = BigSampler.diceElement(i, hasher1.hash(), 1.0)
    val hasher2 = newTestFarmHasher()
    hasher2.putInt(i)
    val dice2 = BigSampler.diceElement(i, hasher2.hash(), 1.0)
    dice1 == dice2
  }

  private val negativeHashCodeStringGen: Gen[String] = Gen.alphaStr
    .suchThat(str => { newTestFarmHasher().putString(str, BigSampler.utf8Charset).hash().asLong < 0} )
  property("dice on hash values that return negative Long representations should still bucket so " +
    "that all values are returned at 100% and none are at 0% " +
    "(should respect modular arithmetic to split when the Long is negative, the same as it would " +
    "if it was positive)") =
    forAll(negativeHashCodeStringGen){ s =>
      val hash = newTestFarmHasher().putString(s, BigSampler.utf8Charset).hash()
      BigSampler.diceElement(s, hash, 100).contains(s)
      BigSampler.diceElement(s, hash, 0) == None
    }

  property("farm hasher should return the same hash all the time - no seed in FarmHash") =
    forAll { i: Int =>
      val hasher1 = newTestFarmHasher()
      val hasher2 = newTestFarmHasher()
      hasher1.putInt(i).hash() == hasher2.putInt(i).hash()
  }
  property("kiss hasher should returns different hash on different seed") = forAll { i: Int =>
    val hasher1 = newTestKissHasher()
    val hasher2 = newTestKissHasher(testSeed.map(_ + 1))
    hasher1.putInt(i).hash() != hasher2.putInt(i).hash()
  }

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
    val hasher = newTestFarmHasher()
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
    val hasher = newTestFarmHasher()
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
    val hasher = newTestFarmHasher()
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

  private val avroSchema = TestRecord.SCHEMA$
  private val richAvroGen = specificRecordOf[TestRecord]
  private val supportedAvroTypes = Seq(
    "int_field",
    "float_field",
    "boolean_field",
    "string_field",
    "long_field",
    "double_field")

  property("should hash on supported required types in Avro") = forAll(richAvroGen) { r =>
    val hasher = newTestFarmHasher()
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
    val hasher = newTestFarmHasher()
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
    val hasher = newTestFarmHasher()
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
        val tbGen =
          richTableRowGen
            .amend(
              Gen.const(i.get(r).asInstanceOf[GenericRecord].get(f))
            )(_.getRecord(r).set(f))
        (i, tbGen.sample.get, s"$r.$f")
    }) { case (avro, tblRow, f) =>
      val tblHash = BigSampler.hashTableRow(tblRow, f, tblSchemaFields, newTestFarmHasher()).hash()
      val avroHash = BigSampler.hashAvroField(avro, f, avroSchema, newTestFarmHasher()).hash()
      tblHash == avroHash
    }

  property("hash of the same fields from the same record should match") = forAll(
    Gen.zip(richAvroGen, Gen.someOf(supportedCommonFields), Gen.oneOf(records)).map {
      case (i, fs, r) =>
        val tbGen = fs.foldLeft(richTableRowGen)((gen, f) => gen.amend(
          Gen.const(i.get(r).asInstanceOf[GenericRecord].get(f)))(_.getRecord(r).set(f)))
        (i, tbGen.sample.get, fs.map(f => s"$r.$f"))
    }) { case (avro, tblRow, fs) =>
    val hashes = fs.foldLeft((newTestFarmHasher(), newTestFarmHasher())){ case ((h1, h2), f) =>
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
    val hashes = fields.foldLeft((newTestFarmHasher(), newTestFarmHasher())){ case ((h1, h2), f) =>
      (BigSampler.hashTableRow(tblRow, f, tblSchemaFields, h1),
        BigSampler.hashAvroField(avro, f, avroSchema, h2))
    }
    hashes._1.hash() == hashes._2.hash()
  }

}

class BigSamplerJobTest extends FlatSpec with Matchers with BeforeAndAfterAllConfigMap {

  val schema = Schemas.avroSchema
  val data1 = Gen.listOfN(20000, genericRecordOf(schema))
    .pureApply(Gen.Parameters.default.withSize(5), Seed.random())
  val data2 = Gen.listOfN(5000, genericRecordOf(schema))
    .pureApply(Gen.Parameters.default.withSize(5), Seed.random())
  val totalElements = 25000
  val dir = Files.createTempDirectory("ratatool-big-sampler-input")
  val file1 = new File(dir.toString, "part-00000.avro")
  val file2 = new File(dir.toString, "part-00001.avro")

  override protected def beforeAll(configMap: ConfigMap): Unit = {
    AvroIO.writeToFile(data1, schema, file1)
    AvroIO.writeToFile(data2, schema, file2)

    dir.toFile.deleteOnExit()
    file1.deleteOnExit()
    file2.deleteOnExit()
  }

  private def withOutFile(testCode: (File) => Any) {
    val outDir = Files.createTempDirectory("ratatool-big-sampler-output").toFile
    try {
      testCode(outDir)
    } finally {
      outDir.delete()
    }
  }
  private def getNumOfAvroRecords(p: String): Long =
    FileStorage(p).listFiles.foldLeft(0)((i, m) =>
      i + AvroIO.readFromFile[GenericRecord](m.resourceId.toString).size)

  "BigSampler" should "work for 50%" in withOutFile { outDir =>
    BigSampler.run(Array(s"--input=$dir/*.avro", s"--output=$outDir", "--sample=0.5"))
    getNumOfAvroRecords(s"$outDir/*.avro").toDouble shouldBe totalElements * 0.5 +- 500
  }

  it should "work for 1%" in withOutFile { outDir =>
    BigSampler.run(Array(s"--input=$dir/*.avro", s"--output=$outDir", "--sample=0.01"))
    getNumOfAvroRecords(s"$outDir/*.avro").toDouble shouldBe totalElements * 0.01 +- 50
  }

  it should "work for 100%" in withOutFile { outDir =>
    BigSampler.run(Array(s"--input=$dir/*.avro", s"--output=$outDir", "--sample=1.0"))
    getNumOfAvroRecords(s"$outDir/*.avro") shouldBe totalElements
  }

  it should "work for 50% with hash field and seed" in withOutFile { outDir =>
    BigSampler.run(Array(
      s"--input=$dir/*.avro",
      s"--output=$outDir",
      "--sample=0.5",
      "--fields=required_fields.int_field"))
    getNumOfAvroRecords(s"$outDir/*.avro").toDouble shouldBe totalElements * 0.5 +- 5000
  }
}

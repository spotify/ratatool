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
import com.spotify.ratatool.io.{AvroIO, FileStorage, ParquetIO}
import com.spotify.ratatool.samplers.util.{ByteHasher, HexEncoding, MurmurHash}
import com.spotify.ratatool.scalacheck._
import org.apache.avro.generic.GenericRecord
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.Hasher
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.BaseEncoding
import org.scalacheck.Gen
import org.scalacheck.rng.Seed
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAllConfigMap, ConfigMap}
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import java.io.File
import java.nio.ByteBuffer
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._
import scala.language.postfixOps

class BigSamplerTest extends AnyFlatSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  private val testSeed = Some(42)
  private def newTestFarmHasher() = BigSampler.hashFun()
  private def newTestKissHasher(testSeed: Option[Int] = testSeed) =
    BigSampler.hashFun(MurmurHash, seed = testSeed)

  /* using the same hasher for more than one test causes BufferOverflows,
   * gen a => Hasher not a Hasher
   */
  private val randomHashGen = Gen.oneOf(() => newTestFarmHasher(), () => newTestKissHasher())

  "BigSampler" should "dice on the same element should match" in {
    forAll { i: Int =>
      val hasher1 = newTestFarmHasher()
      hasher1.putInt(i)
      val dice1 = BigSampler.diceElement(i, hasher1.hash(), 0.01)
      val hasher2 = newTestFarmHasher()
      hasher2.putInt(i)
      val dice2 = BigSampler.diceElement(i, hasher2.hash(), 0.01)
      dice1 shouldBe dice2
    }
  }

  it should "boundLong should be between 0 and 1" in {
    forAll { i: Long =>
      val boundedLong = BigSampler.boundLong(i)
      boundedLong should (be >= 0.0 and be <= 1.0)
    }
  }

  private val negativeHashCodeStringGen: Gen[(String, Hasher)] = for {
    hasher <- randomHashGen
    str <- Gen.alphaNumStr.suchThat { str =>
      hasher().putString(str, BigSampler.utf8Charset).hash().asLong < 0
    }
  } yield (str, hasher())

  it should
    "dice on hash values that return negative Long representations should still bucket so " +
    "that all values are returned at 100% and none are at 0%. This should respect modular " +
    "arithmetic to split when the Long is negative, the same as it would " +
    "if it was positive." in {
      forAll(negativeHashCodeStringGen) { case (s, hasher) =>
        val hash = hasher.putString(s, BigSampler.utf8Charset).hash()
        BigSampler.diceElement(s, hash, 1.0) shouldBe Some(s)
        BigSampler.diceElement(s, hash, 0) shouldBe None
      }
    }

  it should "farm hasher should return the same hash all the time - no seed in FarmHash" in {
    forAll { i: Int =>
      val hasher1 = newTestFarmHasher()
      val hasher2 = newTestFarmHasher()
      hasher1.putInt(i).hash() shouldBe hasher2.putInt(i).hash()
    }
  }

  it should "kiss hasher should returns different hash on different seed" in {
    forAll { i: Int =>
      val hasher1 = newTestKissHasher()
      val hasher2 = newTestKissHasher(testSeed.map(_ + 1))
      hasher1.putInt(i).hash() should not be hasher2.putInt(i).hash()
    }
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
    "datetime_field"
  )

  private val richTableRowHasherGen = for {
    tr <- richTableRowGen
    hasher <- randomHashGen
  } yield (tr, hasher())

  it should "hash on supported types in TableRow" in {
    forAll(richTableRowHasherGen) { case (r, hasher) =>
      supportedTableRowTypes
        .map(t => s"required_fields.$t")
        .foreach { f =>
          noException shouldBe thrownBy(
            BigSampler.hashTableRow(r, f, tblSchemaFields.toList, hasher)
          )
        }
    }
  }

  it should "hash on supported nullable types in TableRow" in {
    forAll(richTableRowHasherGen) { case (r, hasher) =>
      supportedTableRowTypes
        .map(t => s"nullable_fields.$t")
        .foreach { f =>
          noException shouldBe thrownBy(
            BigSampler.hashTableRow(r, f, tblSchemaFields.toList, hasher)
          )
        }
    }
  }

  it should "hash on supported repeated types in TableRow" in {
    forAll(richTableRowHasherGen) { case (r, hasher) =>
      supportedTableRowTypes
        .map(t => s"repeated_fields.$t")
        .foreach { f =>
          noException shouldBe thrownBy(
            BigSampler.hashTableRow(r, f, tblSchemaFields.toList, hasher)
          )
        }
    }
  }

  private val avroSchema = TestRecord.getClassSchema
  private val specificAvroGen = specificRecordOf[TestRecord]
  private val supportedAvroTypes = Seq(
    "int_field",
    "float_field",
    "boolean_field",
    "string_field",
    "long_field",
    "double_field",
    "enum_field",
    "bytes_field",
    "fixed_field"
  )

  private val richAvroHasherGen = for {
    r <- specificAvroGen
    hasher <- randomHashGen
  } yield (r, hasher())

  it should "should hash on supported required specific types" in {
    forAll(richAvroHasherGen) { case (r, hasher) =>
      supportedAvroTypes
        .map(t => s"required_fields.$t")
        .foreach { f =>
          noException shouldBe thrownBy(BigSampler.hashAvroField(r, f, avroSchema, hasher))
        }
    }
  }

  it should "hash on supported nullable specific types" in {
    forAll(richAvroHasherGen) { case (r, hasher) =>
      supportedAvroTypes
        .map(t => s"nullable_fields.$t")
        .foreach { f =>
          noException shouldBe thrownBy(BigSampler.hashAvroField(r, f, avroSchema, hasher))
        }
    }
  }

  it should "hash on supported repeated specific types" in {
    forAll(richAvroHasherGen) { case (r, hasher) =>
      supportedAvroTypes
        .map(t => s"repeated_fields.$t")
        .foreach { f =>
          noException shouldBe thrownBy(BigSampler.hashAvroField(r, f, avroSchema, hasher))
        }
    }
  }

  private val genericAvroGen = genericRecordOf(TestRecord.SCHEMA$)
  private val genericAvroHasherGen = for {
    r <- genericAvroGen
    hasher <- randomHashGen
  } yield (r, hasher())

  it should "hash on supported required generic types" in {
    forAll(genericAvroHasherGen) { case (r, hasher) =>
      supportedAvroTypes
        .map(t => s"required_fields.$t")
        .foreach { f =>
          noException shouldBe thrownBy(BigSampler.hashAvroField(r, f, avroSchema, hasher))
        }
    }
  }

  it should "hash on supported nullable generic types" in {
    forAll(genericAvroHasherGen) { case (r, hasher) =>
      supportedAvroTypes
        .map(t => s"nullable_fields.$t")
        .foreach { f =>
          noException shouldBe thrownBy(BigSampler.hashAvroField(r, f, avroSchema, hasher))
        }
    }
  }

  it should "hash on supported repeated generic types" in {
    forAll(genericAvroHasherGen) { case (r, hasher) =>
      supportedAvroTypes
        .map(t => s"repeated_fields.$t")
        .foreach { f =>
          noException shouldBe thrownBy(BigSampler.hashAvroField(r, f, avroSchema, hasher))
        }
    }
  }

  private val supportedCommonFields =
    Seq("int_field", "float_field", "boolean_field", "string_field")
  private val records = Seq("nullable_fields", "repeated_fields", "required_fields")

  it should "hash of the same single field should match, farmhash" in {
    forAll(Gen.zip(specificAvroGen, Gen.oneOf(supportedCommonFields), Gen.oneOf(records)).map {
      case (i, f, r) =>
        val tbGen =
          richTableRowGen
            .amend(
              Gen.const(i.get(r).asInstanceOf[GenericRecord].get(f))
            )(_.getRecord(r).set(f))
        (i, tbGen.sample.get, s"$r.$f")
    }) { case (avro, tblRow, f) =>
      val tblHash = BigSampler
        .hashTableRow(tblRow, f, tblSchemaFields.toList, newTestFarmHasher())
        .hash()
      val avroHash = BigSampler.hashAvroField(avro, f, avroSchema, newTestFarmHasher()).hash()
      tblHash shouldBe avroHash
    }
  }

  it should "hash of the same single field should match, kisshash" in {
    forAll(Gen.zip(specificAvroGen, Gen.oneOf(supportedCommonFields), Gen.oneOf(records)).map {
      case (i, f, r) =>
        val tbGen =
          richTableRowGen
            .amend(
              Gen.const(i.get(r).asInstanceOf[GenericRecord].get(f))
            )(_.getRecord(r).set(f))
        (i, tbGen.sample.get, s"$r.$f")
    }) { case (avro, tblRow, f) =>
      val tblHash = BigSampler
        .hashTableRow(tblRow, f, tblSchemaFields.toList, newTestKissHasher())
        .hash()
      val avroHash = BigSampler.hashAvroField(avro, f, avroSchema, newTestKissHasher()).hash()
      tblHash shouldBe avroHash
    }
  }

  it should "hash of the same fields from the same record should match, farmHash" in {
    forAll(Gen.zip(specificAvroGen, Gen.someOf(supportedCommonFields), Gen.oneOf(records)).map {
      case (i, fs, r) =>
        val tbGen = fs.foldLeft(richTableRowGen)((gen, f) =>
          gen.amend(Gen.const(i.get(r).asInstanceOf[GenericRecord].get(f)))(_.getRecord(r).set(f))
        )
        (i, tbGen.sample.get, fs.map(f => s"$r.$f"))
    }) { case (avro, tblRow, fs) =>
      val hashes = fs.foldLeft((newTestFarmHasher(), newTestFarmHasher())) { case ((h1, h2), f) =>
        (
          BigSampler.hashTableRow(tblRow, f, tblSchemaFields.toList, h1),
          BigSampler.hashAvroField(avro, f, avroSchema, h2)
        )
      }
      hashes._1.hash() shouldBe hashes._2.hash()
    }
  }

  it should "hash of the same fields from the same record should match, kissHash" in {
    forAll(Gen.zip(specificAvroGen, Gen.someOf(supportedCommonFields), Gen.oneOf(records)).map {
      case (i, fs, r) =>
        val tbGen = fs.foldLeft(richTableRowGen)((gen, f) =>
          gen.amend(Gen.const(i.get(r).asInstanceOf[GenericRecord].get(f)))(_.getRecord(r).set(f))
        )
        (i, tbGen.sample.get, fs.map(f => s"$r.$f"))
    }) { case (avro, tblRow, fs) =>
      val hashes = fs.foldLeft((newTestKissHasher(), newTestKissHasher())) { case ((h1, h2), f) =>
        (
          BigSampler.hashTableRow(tblRow, f, tblSchemaFields.toList, h1),
          BigSampler.hashAvroField(avro, f, avroSchema, h2)
        )
      }
      hashes._1.hash() shouldBe hashes._2.hash()
    }
  }

  it should "hash of the same fields from multiple record should match, farmHash" in {
    forAll(
      Gen
        .zip(
          specificAvroGen,
          Gen.someOf(supportedCommonFields).suchThat(_.nonEmpty),
          Gen.someOf(records).suchThat(_.nonEmpty)
        )
        .map { case (i, fs, rs) =>
          val crossFieldToRecord = for {
            f <- fs
            r <- rs
          } yield (f, r)
          val tbGen = crossFieldToRecord.foldLeft(richTableRowGen) { case (gen, (f, r)) =>
            gen.amend(Gen.const(i.get(r).asInstanceOf[GenericRecord].get(f)))(_.getRecord(r).set(f))
          }
          (i, tbGen.sample.get, crossFieldToRecord.map { case (f, r) => s"$r.$f" })
        }
    ) { case (avro, tblRow, fields) =>
      val hashes = fields.foldLeft((newTestFarmHasher(), newTestFarmHasher())) {
        case ((h1, h2), f) =>
          (
            BigSampler.hashTableRow(tblRow, f, tblSchemaFields.toList, h1),
            BigSampler.hashAvroField(avro, f, avroSchema, h2)
          )
      }
      hashes._1.hash() shouldBe hashes._2.hash()
    }
  }

  it should "hash of the same fields from multiple record should match, kissHash" in {
    forAll(
      Gen
        .zip(
          specificAvroGen,
          Gen.someOf(supportedCommonFields).suchThat(_.nonEmpty),
          Gen.someOf(records).suchThat(_.nonEmpty)
        )
        .map { case (i, fs, rs) =>
          val crossFieldToRecord = for {
            f <- fs
            r <- rs
          } yield (f, r)
          val tbGen = crossFieldToRecord.foldLeft(richTableRowGen) { case (gen, (f, r)) =>
            gen.amend(Gen.const(i.get(r).asInstanceOf[GenericRecord].get(f)))(_.getRecord(r).set(f))
          }
          (i, tbGen.sample.get, crossFieldToRecord.map { case (f, r) => s"$r.$f" })
        }
    ) { case (avro, tblRow, fields) =>
      val hashes = fields.foldLeft((newTestKissHasher(), newTestKissHasher())) {
        case ((h1, h2), f) =>
          (
            BigSampler.hashTableRow(tblRow, f, tblSchemaFields.toList, h1),
            BigSampler.hashAvroField(avro, f, avroSchema, h2)
          )
      }
      hashes._1.hash() shouldBe hashes._2.hash()
    }
  }

  it should "hashes of bytes, hex strings, and fixed should be the same" in {
    forAll(specificAvroGen) { case avro =>
      val hexEncode = BaseEncoding.base16.lowerCase
      avro.getRequiredFields.setBytesField(
        ByteBuffer.wrap(avro.getRequiredFields.getFixedField.bytes)
      )
      avro.getRequiredFields.setStringField(
        hexEncode.encode(avro.getRequiredFields.getFixedField.bytes)
      )

      val hasher1 =
        ByteHasher.wrap(newTestKissHasher(Some(0)), HexEncoding, BigSampler.utf8Charset)
      val bytesHash =
        BigSampler.hashAvroField(avro, "required_fields.bytes_field", avroSchema, hasher1).hash()

      val hasher2 =
        ByteHasher.wrap(newTestKissHasher(Some(0)), HexEncoding, BigSampler.utf8Charset)
      val fixedHash =
        BigSampler.hashAvroField(avro, "required_fields.fixed_field", avroSchema, hasher2).hash()

      val hasher3 = newTestKissHasher(Some(0))
      val stringHash =
        BigSampler.hashAvroField(avro, "required_fields.string_field", avroSchema, hasher3).hash()

      bytesHash shouldBe fixedHash
      fixedHash shouldBe stringHash
    }
  }
}

/**
 * Base testing class for BigSampler. Adding a new subclass here rqeuires also adding a line to test
 * it in travis.yml
 */
sealed trait BigSamplerJobTestRoot
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAllConfigMap {
  val schema = Schemas.avroSchema
  def data1Size: Int
  def data2Size: Int
  def totalElements: Int = data1Size + data2Size

  val data1: List[GenericRecord] = Gen
    .listOfN(data1Size, genericRecordOf(schema))
    .pureApply(Gen.Parameters.default.withSize(5), Seed.random())
    .map { gr =>
      val req = gr.get("required_fields").asInstanceOf[GenericRecord]
      req.put("string_field", "large_strata")
      gr.put("required_fields", req)
      gr
    }
  val data2: List[GenericRecord] = Gen
    .listOfN(data2Size, genericRecordOf(schema))
    .pureApply(Gen.Parameters.default.withSize(5), Seed.random())
    .map { gr =>
      val req = gr.get("required_fields").asInstanceOf[GenericRecord]
      req.put("string_field", "small_strata")
      gr.put("required_fields", req)
      gr
    }
  val dir: Path = Files.createTempDirectory("ratatool-big-sampler-input")
  val file1 = new File(dir.toString, "part-00000.avro")
  val file2 = new File(dir.toString, "part-00001.avro")
  val fileParquet1 = new File(dir.toString, "part-00000.parquet")
  val fileParquet2 = new File(dir.toString, "part-00001.parquet")

  override protected def beforeAll(configMap: ConfigMap): Unit = {
    AvroIO.writeToFile(data1, schema, file1)
    AvroIO.writeToFile(data2, schema, file2)
    ParquetIO.writeToFile(data1, schema, fileParquet1)
    ParquetIO.writeToFile(data2, schema, fileParquet2)

    dir.toFile.deleteOnExit()
    file1.deleteOnExit()
    file2.deleteOnExit()
    fileParquet1.deleteOnExit()
    fileParquet2.deleteOnExit()
  }

  protected def withOutFile(testCode: (File) => Any) {
    val outDir = Files.createTempDirectory("ratatool-big-sampler-output").toFile
    try {
      testCode(outDir)
    } finally {
      outDir.delete()
    }
  }

  protected def countAvroRecords(p: String, f: GenericRecord => Boolean = _ => true): Long =
    FileStorage(p).listFiles.foldLeft(0)((i, m) =>
      i + AvroIO.readFromFile[GenericRecord](m.resourceId().toString).count(f)
    )

  protected def countParquetRecords(p: String, f: GenericRecord => Boolean = _ => true): Long =
    FileStorage(p).listFiles.foldLeft(0)((i, m) =>
      i + ParquetIO.readFromFile(m.resourceId().toString).count(f)
    )
}

class BigSamplerBasicJobTest extends BigSamplerJobTestRoot {
  override def data1Size: Int = 20000
  override def data2Size: Int = 5000

  "BigSampler" should "work for 50%" in withOutFile { outDir =>
    BigSampler.run(Array(s"--input=$dir/*.avro", s"--output=$outDir", "--sample=0.5"))
    countAvroRecords(s"$outDir/*.avro").toDouble shouldBe totalElements * 0.5 +- 250
  }

  it should "work for 50% for parquet" in withOutFile { outDir =>
    BigSampler.run(Array(s"--input=$dir/*.parquet", s"--output=$outDir", "--sample=0.5"))
    countParquetRecords(s"$outDir/*.parquet").toDouble shouldBe totalElements * 0.5 +- 250
  }

  it should "work for 1%" in withOutFile { outDir =>
    BigSampler.run(Array(s"--input=$dir/*.avro", s"--output=$outDir", "--sample=0.01"))
    countAvroRecords(s"$outDir/*.avro").toDouble shouldBe totalElements * 0.01 +- 35
  }

  it should "work for 1% for parquet" in withOutFile { outDir =>
    BigSampler.run(Array(s"--input=$dir/*.parquet", s"--output=$outDir", "--sample=0.01"))
    countParquetRecords(s"$outDir/*.parquet").toDouble shouldBe totalElements * 0.01 +- 35
  }

  it should "work for 100%" in withOutFile { outDir =>
    BigSampler.run(Array(s"--input=$dir/*.avro", s"--output=$outDir", "--sample=1.0"))
    countAvroRecords(s"$outDir/*.avro") shouldBe totalElements
  }

  it should "work for 100% for parquet" in withOutFile { outDir =>
    BigSampler.run(Array(s"--input=$dir/*.parquet", s"--output=$outDir", "--sample=1.0"))
    countParquetRecords(s"$outDir/*.parquet").toDouble shouldBe totalElements
  }

  it should "work for 50% with hash field and seed" in withOutFile { outDir =>
    BigSampler.run(
      Array(
        s"--input=$dir/*.avro",
        s"--output=$outDir",
        "--sample=0.5",
        "--seed=42",
        "--fields=required_fields.int_field"
      )
    )
    countAvroRecords(s"$outDir/*.avro").toDouble shouldBe totalElements * 0.5 +- 2000
  }

  it should "work for 50% with hash field and seed for parquet" in withOutFile { outDir =>
    BigSampler.run(
      Array(
        s"--input=$dir/*.parquet",
        s"--output=$outDir",
        "--sample=0.5",
        "--seed=42",
        "--fields=required_fields.int_field"
      )
    )
    countParquetRecords(s"$outDir/*.parquet").toDouble shouldBe totalElements * 0.5 +- 2000
  }
}

class BigSamplerWildCardTest extends BigSamplerJobTestRoot {
  override def data1Size: Int = 10000
  override def data2Size: Int = 2500

  override protected def beforeAll(configMap: ConfigMap): Unit = {
    ParquetIO.writeToFile(data1, schema, fileParquet1)
    ParquetIO.writeToFile(data2, schema, fileParquet2)

    dir.toFile.deleteOnExit()
    fileParquet1.deleteOnExit()
    fileParquet2.deleteOnExit()
  }

  "BigSampler" should "work for wildcard without file extension" in withOutFile { outDir =>
    BigSampler.run(Array(s"--input=$dir/part-*", s"--output=$outDir", "--sample=0.5"))
    countParquetRecords(s"$outDir/*.parquet").toDouble shouldBe totalElements * 0.5 +- 250
  }
}

class BigSamplerApproxDistJobTest extends BigSamplerJobTestRoot {
  override def data1Size: Int = 10000
  override def data2Size: Int = 2500

  "BigSampler" should "stratify across a single field" in withOutFile { outDir =>
    BigSampler.run(
      Array(
        s"--input=$dir/*.avro",
        s"--output=$outDir",
        "--sample=0.5",
        "--distribution=stratified",
        "--distributionFields=required_fields.string_field"
      )
    )
    val largeStrataCount = countAvroRecords(
      s"$outDir/*.avro",
      (gr: GenericRecord) =>
        gr.get("required_fields")
          .asInstanceOf[GenericRecord]
          .get("string_field")
          .toString == "large_strata"
    ).toDouble
    val smallStrataCount = countAvroRecords(
      s"$outDir/*.avro",
      (gr: GenericRecord) =>
        gr.get("required_fields")
          .asInstanceOf[GenericRecord]
          .get("string_field")
          .toString == "small_strata"
    ).toDouble
    val totalCount = countAvroRecords(s"$outDir/*.avro").toDouble
    totalCount shouldBe totalElements * 0.5 +- 250
    largeStrataCount / totalCount shouldBe (data1Size.toDouble / totalElements) +- 0.05
    smallStrataCount / totalCount shouldBe (data2Size.toDouble / totalElements) +- 0.05
  }

  it should "stratify across a single field with hash field" in withOutFile { outDir =>
    BigSampler.run(
      Array(
        s"--input=$dir/*.avro",
        s"--output=$outDir",
        "--sample=0.5",
        "--distribution=stratified",
        "--distributionFields=required_fields.string_field",
        "--fields=required_fields.long_field,required_fields.int_field"
      )
    )
    val largeStrataCount = countAvroRecords(
      s"$outDir/*.avro",
      (gr: GenericRecord) =>
        gr.get("required_fields")
          .asInstanceOf[GenericRecord]
          .get("string_field")
          .toString == "large_strata"
    ).toDouble
    val smallStrataCount = countAvroRecords(
      s"$outDir/*.avro",
      (gr: GenericRecord) =>
        gr.get("required_fields")
          .asInstanceOf[GenericRecord]
          .get("string_field")
          .toString == "small_strata"
    ).toDouble
    val totalCount = countAvroRecords(s"$outDir/*.avro").toDouble
    totalCount shouldBe totalElements * 0.5 +- 2000
    largeStrataCount / totalCount shouldBe (data1Size.toDouble / totalElements) +- 0.05
    smallStrataCount / totalCount shouldBe (data2Size.toDouble / totalElements) +- 0.05
  }

  it should "sample uniformly across a single field" in withOutFile { outDir =>
    BigSampler.run(
      Array(
        s"--input=$dir/*.avro",
        s"--output=$outDir",
        "--sample=0.1",
        "--distribution=uniform",
        "--distributionFields=required_fields.string_field"
      )
    )
    val largeStrataCount = countAvroRecords(
      s"$outDir/*.avro",
      (gr: GenericRecord) =>
        gr.get("required_fields")
          .asInstanceOf[GenericRecord]
          .get("string_field")
          .toString == "large_strata"
    ).toDouble
    val smallStrataCount = countAvroRecords(
      s"$outDir/*.avro",
      (gr: GenericRecord) =>
        gr.get("required_fields")
          .asInstanceOf[GenericRecord]
          .get("string_field")
          .toString == "small_strata"
    ).toDouble
    val totalCount = countAvroRecords(s"$outDir/*.avro").toDouble
    totalCount shouldBe totalElements * 0.1 +- 750
    largeStrataCount / totalCount shouldBe 0.5 +- 0.05
    smallStrataCount / totalCount shouldBe 0.5 +- 0.05
  }

  it should "sample uniformly across a single field with hash field" in withOutFile { outDir =>
    BigSampler.run(
      Array(
        s"--input=$dir/*.avro",
        s"--output=$outDir",
        "--sample=0.1",
        "--distribution=uniform",
        "--distributionFields=required_fields.string_field",
        "--fields=required_fields.long_field,required_fields.int_field"
      )
    )
    val largeStrataCount = countAvroRecords(
      s"$outDir/*.avro",
      (gr: GenericRecord) =>
        gr.get("required_fields")
          .asInstanceOf[GenericRecord]
          .get("string_field")
          .toString == "large_strata"
    ).toDouble
    val smallStrataCount = countAvroRecords(
      s"$outDir/*.avro",
      (gr: GenericRecord) =>
        gr.get("required_fields")
          .asInstanceOf[GenericRecord]
          .get("string_field")
          .toString == "small_strata"
    ).toDouble
    val totalCount = countAvroRecords(s"$outDir/*.avro").toDouble
    totalCount shouldBe totalElements * 0.1 +- 750
    largeStrataCount / totalCount shouldBe 0.5 +- 0.1
    smallStrataCount / totalCount shouldBe 0.5 +- 0.1
  }
}

class BigSamplerExactDistJobTest extends BigSamplerJobTestRoot {
  override def data1Size: Int = 5000
  override def data2Size: Int = 1250

  "BigSampler" should "stratify across a single field exactly" in withOutFile { outDir =>
    BigSampler.run(
      Array(
        s"--input=$dir/*.avro",
        s"--output=$outDir",
        "--sample=0.4",
        "--distribution=stratified",
        "--distributionFields=required_fields.string_field"
      )
    )
    val largeStrataCount = countAvroRecords(
      s"$outDir/*.avro",
      (gr: GenericRecord) =>
        gr.get("required_fields")
          .asInstanceOf[GenericRecord]
          .get("string_field")
          .toString == "large_strata"
    ).toDouble
    val smallStrataCount = countAvroRecords(
      s"$outDir/*.avro",
      (gr: GenericRecord) =>
        gr.get("required_fields")
          .asInstanceOf[GenericRecord]
          .get("string_field")
          .toString == "small_strata"
    ).toDouble
    val totalCount = countAvroRecords(s"$outDir/*.avro").toDouble
    totalCount shouldBe totalElements * 0.4 +- 125
    largeStrataCount / totalCount shouldBe (data1Size.toDouble / totalElements) +- 0.02
    smallStrataCount / totalCount shouldBe (data2Size.toDouble / totalElements) +- 0.02
  }

  it should "stratify across a single field with hash field exactly" in withOutFile { outDir =>
    BigSampler.run(
      Array(
        s"--input=$dir/*.avro",
        s"--output=$outDir",
        "--sample=0.5",
        "--distribution=stratified",
        "--distributionFields=required_fields.string_field",
        "--fields=required_fields.long_field,required_fields.int_field",
        "--exact"
      )
    )
    val largeStrataCount = countAvroRecords(
      s"$outDir/*.avro",
      (gr: GenericRecord) =>
        gr.get("required_fields")
          .asInstanceOf[GenericRecord]
          .get("string_field")
          .toString == "large_strata"
    ).toDouble
    val smallStrataCount = countAvroRecords(
      s"$outDir/*.avro",
      (gr: GenericRecord) =>
        gr.get("required_fields")
          .asInstanceOf[GenericRecord]
          .get("string_field")
          .toString == "small_strata"
    ).toDouble
    val totalCount = countAvroRecords(s"$outDir/*.avro").toDouble
    totalCount shouldBe totalElements * 0.5 +- 150
    largeStrataCount / totalCount shouldBe (data1Size.toDouble / totalElements) +- 0.02
    smallStrataCount / totalCount shouldBe (data2Size.toDouble / totalElements) +- 0.02
  }

  it should "sample uniformly across a single field exactly" in withOutFile { outDir =>
    BigSampler.run(
      Array(
        s"--input=$dir/*.avro",
        s"--output=$outDir",
        "--sample=0.25",
        "--distribution=uniform",
        "--distributionFields=required_fields.string_field",
        "--exact"
      )
    )
    val largeStrataCount = countAvroRecords(
      s"$outDir/*.avro",
      (gr: GenericRecord) =>
        gr.get("required_fields")
          .asInstanceOf[GenericRecord]
          .get("string_field")
          .toString == "large_strata"
    ).toDouble
    val smallStrataCount = countAvroRecords(
      s"$outDir/*.avro",
      (gr: GenericRecord) =>
        gr.get("required_fields")
          .asInstanceOf[GenericRecord]
          .get("string_field")
          .toString == "small_strata"
    ).toDouble
    val totalCount = countAvroRecords(s"$outDir/*.avro").toDouble
    totalCount shouldBe totalElements * 0.25 +- 125
    largeStrataCount / totalCount shouldBe 0.5 +- 0.02
    smallStrataCount / totalCount shouldBe 0.5 +- 0.02
  }

  it should "sample uniformly across a single field with hash exactly" in withOutFile { outDir =>
    BigSampler.run(
      Array(
        s"--input=$dir/*.avro",
        s"--output=$outDir",
        "--sample=0.2",
        "--distribution=uniform",
        "--distributionFields=required_fields.string_field",
        "--fields=required_fields.long_field,required_fields.int_field",
        "--exact"
      )
    )
    val largeStrataCount = countAvroRecords(
      s"$outDir/*.avro",
      (gr: GenericRecord) =>
        gr.get("required_fields")
          .asInstanceOf[GenericRecord]
          .get("string_field")
          .toString == "large_strata"
    ).toDouble
    val smallStrataCount = countAvroRecords(
      s"$outDir/*.avro",
      (gr: GenericRecord) =>
        gr.get("required_fields")
          .asInstanceOf[GenericRecord]
          .get("string_field")
          .toString == "small_strata"
    ).toDouble
    val totalCount = countAvroRecords(s"$outDir/*.avro").toDouble
    totalCount shouldBe totalElements * 0.2 +- 125
    largeStrataCount / totalCount shouldBe 0.5 +- 0.025
    smallStrataCount / totalCount shouldBe 0.5 +- 0.025
  }
}

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

package com.spotify.ratatool.examples

import com.google.api.services.bigquery.model.TableRow
import com.spotify.ratatool.examples.scalacheck.ExampleTableRowGen
import com.spotify.ratatool.scalacheck._
import org.scalacheck.Gen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class ExampleTableRowGenTest extends AnyFlatSpec with Matchers with ScalaCheckDrivenPropertyChecks {
  val gen: Gen[TableRow] = ExampleTableRowGen.tableRowGen
  val listGen: Gen[List[TableRow]] = Gen.listOfN(1000, gen)

  "ExampleTableRowGenerator" should "generates Foo and Bar more frequently than Fizz and Buzz" in {
    forAll(listGen) { l =>
      val stringFields: Seq[String] = l.flatMap { r =>
        Option(r.getRecord("nullable_record"))
          .map(_.get("frequency_string_field").asInstanceOf[String])
      }

      val fooBarCount = stringFields.count(s => s == "Foo" || s == "Bar")
      val fizzBuzzCount = stringFields.count(s => s == "Fizz" || s == "Buzz")
      fooBarCount should be > fizzBuzzCount
    }
  }

  it should "generates valid dependent bytes" in {
    forAll(gen) { r =>
      val s = r.getRecord("required_record").get("independent_string_field").asInstanceOf[String]
      val b = r.getRecord("required_record").get("dependent_bytes_field").asInstanceOf[Array[Byte]]
      new String(b) shouldBe s
    }
  }

  it should "generate same record id when using amend2 generators" in {
    forAll(ExampleTableRowGen.exampleRecordAmend2Gen) { case (gen1, gen2) =>
      gen1.get("record_id") shouldBe gen2.get("record_id")
    }
  }

  it should "generate same record id when using amend2 for correlated fields" in {
    forAll(ExampleTableRowGen.correlatedRecordGen) { case correlatedRecord =>
      correlatedRecord.get("record_id") shouldBe correlatedRecord.get("parent_record_id")
    }
  }

}

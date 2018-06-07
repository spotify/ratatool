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
import org.scalacheck.{Gen, Properties}
import org.scalacheck.Prop.{AnyOperators, forAll}


object ExampleTableRowGenTest extends Properties("ExampleTableRowGenerator") {
  val gen: Gen[TableRow] = ExampleTableRowGen.tableRowGen
  val listGen: Gen[List[TableRow]] = Gen.listOfN(1000, gen)

  property("generates Foo and Bar more frequently than Fizz and Buzz") = forAll(listGen) { l =>
    val stringFields: Seq[String] = l.flatMap { r =>
      Option(r.getRecord("nullable_record"))
        .map(_.get("frequency_string_field").asInstanceOf[String])
    }

    stringFields.count(s => s == "Foo" || s == "Bar") >
      stringFields.count(s => s == "Fizz" || s == "Buzz")
  }

  property("generates valid dependent bytes") = forAll(gen) { r =>
    val s = r.getRecord("required_record").get("independent_string_field").asInstanceOf[String]
    val b = r.getRecord("required_record").get("dependent_bytes_field").asInstanceOf[Array[Byte]]
    new String(b) ?= s
  }

  property("the record id is the same when using amend2 generators") =
    forAll(ExampleTableRowGen.exampleRecordAmend2Gen) { case (gen1, gen2) =>
      gen1.get("record_id") == gen2.get("record_id")
    }

  property("the record id is the same when using amend2 for correlated fields") =
    forAll(ExampleTableRowGen.correlatedRecordGen) {
      case (correlatedRecord) => correlatedRecord.get("record_id") ==
        correlatedRecord.get("parent_record_id")
    }

}

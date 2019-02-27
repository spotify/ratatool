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

import java.util.UUID

import com.spotify.ratatool.avro.specific.{EnumField, ExampleRecord}
import com.spotify.ratatool.examples.scalacheck.ExampleAvroGen
import org.scalacheck.{Gen, Properties}
import org.scalacheck.Prop.{AnyOperators, BooleanOperators, forAll}

import scala.collection.JavaConverters._

object ExampleAvroGenTest extends Properties("ExampleAvroGenerator") {
  val gen: Gen[ExampleRecord] = ExampleAvroGen.exampleRecordGen

  property("round trips UUID") = forAll(gen) { m =>
    UUID.fromString(m.getRecordId.toString).toString ?= m.getRecordId.toString
  }

  property("generates valid dependent int") = forAll(gen) { m =>
    (m.getIndependentIntField == 0
      && m.getDependentIntField == Int.MaxValue) :| "Max if indep is 0" ||
    (m.getIndependentIntField != 0
      && m.getDependentIntField == m.getIndependentIntField/2) :| "Half when indep is not 0"
  }

  property("generates valid dependent enum") = forAll(gen) { m =>
    (m.getIndependentStringField.toString.startsWith("Exception") &&
      m.getDependentEnumField == EnumField.Failure) :| "Is Failure on Exception" ||
    (!m.getIndependentStringField.toString.startsWith("Exception") &&
      m.getDependentEnumField == EnumField.Success) :| "Is Success when non-Exception"
  }

  property("double field within bounds") = forAll(gen) { m =>
    m.getBoundedDoubleField <= 1.0 && m.getBoundedDoubleField >= -1.0
  }

  property("map field size within bounds") = forAll(gen) { m =>
    val size = m.getNestedRecordField.getMapField.asScala.size
    size <= 5 && size >= 0
  }

  property("the record id is the same when using amend2 generators") =
    forAll(ExampleAvroGen.exampleRecordAmend2Gen) {
      case (gen1, gen2) => gen1.getRecordId == gen2.getRecordId
    }

  property("the record id is the same when using amend2 for correlated fields") =
    forAll(ExampleAvroGen.correlatedRecordGen) {
      correlatedGen =>
        correlatedGen.getRecordId == correlatedGen.getNestedRecordField.getParentRecordId
    }
}

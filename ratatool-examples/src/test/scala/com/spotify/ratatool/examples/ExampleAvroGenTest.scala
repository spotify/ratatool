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

import com.spotify.ratatool.avro.specific.{EnumField, ExampleRecord}
import com.spotify.ratatool.examples.scalacheck.ExampleAvroGen
import org.scalacheck.Gen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import java.util.UUID
import scala.jdk.CollectionConverters._

class ExampleAvroGenTest extends AnyFlatSpec with Matchers with ScalaCheckDrivenPropertyChecks {
  val gen: Gen[ExampleRecord] = ExampleAvroGen.exampleRecordGen

  "ExampleAvroGenerator" should "round trips UUID" in {
    forAll(gen) { m =>
      UUID.fromString(m.getRecordId.toString).toString shouldBe m.getRecordId.toString
    }
  }

  it should "generates valid dependent int" in {
    forAll(gen) { m =>
      if (m.getIndependentIntField == 0) {
        m.getDependentIntField shouldBe Int.MaxValue
      } else {
        m.getDependentIntField shouldBe (m.getIndependentIntField / 2)
      }
    }
  }

  it should "generates valid dependent enum" in {
    forAll(gen) { m =>
      if (m.getIndependentStringField.toString.startsWith("Exception")) {
        m.getDependentEnumField shouldBe EnumField.Failure
      } else {
        m.getDependentEnumField shouldBe EnumField.Success
      }
    }
  }

  it should "generate double field within bounds" in {
    forAll(gen) { m =>
      m.getBoundedDoubleField.toDouble should (be <= 1.0 and be >= -1.0)
    }
  }

  it should "generate map field size within bounds" in {
    forAll(gen) { m =>
      m.getNestedRecordField.getMapField.asScala.size should (be <= 5 and be >= 0)
    }
  }

  it should "generate same record id using amend2 generators" in {
    forAll(ExampleAvroGen.exampleRecordAmend2Gen) { case (gen1, gen2) =>
      gen1.getRecordId shouldBe gen2.getRecordId
    }
  }

  it should "generate same record id when using amend2 for correlated fields" in {
    forAll(ExampleAvroGen.correlatedRecordGen) { correlatedGen =>
      correlatedGen.getRecordId shouldBe correlatedGen.getNestedRecordField.getParentRecordId
    }
  }
}

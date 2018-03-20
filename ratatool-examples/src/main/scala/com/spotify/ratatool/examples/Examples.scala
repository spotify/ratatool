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

import java.util

import com.spotify.ratatool.avro.specific.{EnumField, ExampleRecord, NestedExampleRecord}
import com.spotify.ratatool.scalacheck._
import org.apache.avro.util.Utf8
import org.scalacheck.{Arbitrary, Gen}

object Examples {
  private val utfGen: Gen[Utf8] = Arbitrary.arbString.arbitrary.map(new Utf8(_))

  private val kvGen: Gen[(Utf8, Int)] = for {
    k <- utfGen
    v <- Arbitrary.arbInt.arbitrary
  } yield (k, v)

  private val sizedMapGen: Gen[util.Map[CharSequence, java.lang.Integer]] =
    Gen.mapOfN(5, kvGen).map { m =>
      val map = new util.HashMap[Utf8, java.lang.Integer]()
      m.foreach{case (k, v) => map.put(k, v)}
      map.asInstanceOf[util.Map[CharSequence, java.lang.Integer]]
    }

  private val nestedRecordGen: Gen[NestedExampleRecord] = specificRecordOf[NestedExampleRecord]
    .amend(sizedMapGen)(_.setMapField)

  private val boundedDoubleGen: Gen[Double] = Gen.chooseNum(-1.0, 1.0)

  def proxy(): Gen[Int] = {
    val proxy2: Gen[Int] = Gen.chooseNum(0, 100)
    proxy2
  }

  private val intGen: Gen[Int] = Gen.chooseNum(0, 100)

  private def dependentIntFunc(i: Int): Int = {
    if (i == 0) {
      Int.MaxValue
    } else {
      i / 2
    }
  }

  private val errorGen: Gen[String] = for {
    e <- Gen.const("Exception: Ratatool Exception. ")
    m <- Gen.alphaNumStr
  } yield e + m

  private val stringGen: Gen[String] = Gen.oneOf(Gen.alphaNumStr, errorGen)

  private def dependentEnumFunc(s: String): EnumField = {
    if (s.length > 0 && s.startsWith("Exception")) {
      EnumField.Failure
    } else {
      EnumField.Success
    }
  }

  /**
   * An example of generating avro data with specific requirements.
   *
   * See [[com.spotify.ratatool.avro.specific.ExampleRecord]] for documentation on field
   * requirements, dependencies, and bounds.
   */
  val exampleRecordGen: Gen[ExampleRecord] =
    specificRecordOf[ExampleRecord]
      .amend(nestedRecordGen)(_.setNestedRecordField)
      .amend(boundedDoubleGen)(_.setBoundedDoubleField)
      .amend(Gen.uuid.map(_.toString))(_.setRecordId)
      .amend(intGen)(m => i => {
        m.setIndependentIntField(i)
        m.setDependentIntField(dependentIntFunc(i))
      })
      .amend(stringGen)(m => s => {
        m.setIndependentStringField(s)
        m.setDependentEnumField(dependentEnumFunc(s))
      })
}

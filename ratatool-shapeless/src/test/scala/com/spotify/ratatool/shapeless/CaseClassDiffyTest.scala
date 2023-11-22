/*
 * Copyright 2016 Spotify AB.
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

package com.spotify.ratatool.shapeless

import com.spotify.ratatool.diffy.{Delta, NumericDelta, StringDelta, UnknownDelta}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CaseClassDiffyTest extends AnyFlatSpec with Matchers {
  case class Foo(f1: String, f2: Int, f3: Long, f4: Seq[Double], f5: Seq[String])
  case class Bar(b1: Double, b2: Foo)

  val f1 = Foo("foo1", 1, 3L, Seq(1, 3), Seq("foo1"))
  val f2 = f1.copy(f1 = "foo2", f4 = Seq(1, 2), f5 = Seq("foo2"))

  val b1 = Bar(1, f1)
  val b2 = Bar(2, f2)

  val dFoo = new CaseClassDiffy[Foo]
  val dBar = new CaseClassDiffy[Bar]

  val ignoreSet = Set("f2")
  val dFooWithIgnore = new CaseClassDiffy[Foo](ignore = ignoreSet)
  val dBarWithIgnore = new CaseClassDiffy[Bar](ignore = ignoreSet)

  "CaseClassDiffy" should "support primitive fields" in {
    val result = dFoo.apply(f1, f2)

    result should contain(Delta("f1", Option("foo1"), Option("foo2"), StringDelta(1.0)))
    result should contain(Delta("f2", Option(1), Option(1), NumericDelta(0.0)))
    result should contain(Delta("f3", Option(3), Option(3), NumericDelta(0.0)))
    result should contain(Delta("f5", Option(Vector("foo1")), Option(Vector("foo2")), UnknownDelta))
  }

  "CaseClassDiffy" should "support nested fields" in {
    val result = dBar.apply(b1, b2)

    result should contain(Delta("b1", Option(1), Option(2), NumericDelta(1.0)))
    result should contain(Delta("b2.f1", Option("foo1"), Option("foo2"), StringDelta(1.0)))
    result should contain(Delta("b2.f2", Option(1), Option(1), NumericDelta(0.0)))
    result should contain(Delta("b2.f3", Option(3), Option(3), NumericDelta(0.0)))
    result should contain
    Delta("b2.f5", Option(Vector("foo1")), Option(Vector("foo2")), UnknownDelta)
  }

  "CaseClassDiffy" should "support ignore with exact match case" in {
    val result = dFooWithIgnore.apply(f1, f2)
    result.map(_.field) shouldNot contain("f2")

    result should contain(Delta("f1", Option("foo1"), Option("foo2"), StringDelta(1.0)))
    result should contain(Delta("f3", Option(3), Option(3), NumericDelta(0.0)))
    result should contain(Delta("f5", Option(Vector("foo1")), Option(Vector("foo2")), UnknownDelta))
  }

  "CaseClassDiffy" should "support ignore with nested field case" in {
    val result = dBarWithIgnore.apply(b1, b2)
    result.map(_.field) shouldNot contain("f2")

    result should contain(Delta("b1", Option(1), Option(2), NumericDelta(1.0)))
    result should contain(Delta("b2.f1", Option("foo1"), Option("foo2"), StringDelta(1.0)))
    result should contain(Delta("b2.f3", Option(3), Option(3), NumericDelta(0.0)))
    result should contain
    Delta("b2.f5", Option(Vector("foo1")), Option(Vector("foo2")), UnknownDelta)
  }
}

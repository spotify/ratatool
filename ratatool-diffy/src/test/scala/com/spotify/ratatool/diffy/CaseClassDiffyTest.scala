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

package com.spotify.ratatool.diffy

import org.scalatest.{FlatSpec, Matchers}
import shapeless._

class CaseClassDiffyTest extends FlatSpec with Matchers {

  case class Inner(innerA: Double)
  case class TestClassRecursive(a: String, b: Int, c: Inner)

  val dr = new CaseClassDiffy(LabelledGeneric[TestClassRecursive])(
    Map(classOf[Inner].toString -> LabelledGeneric[Inner]))

  "CaseClassDiffy" should "do a thing recursively" in {
    dr(TestClassRecursive("foo", 0, Inner(1.0)),
      TestClassRecursive("foo", 0, Inner(1.0))) should equal (Seq())
    dr(TestClassRecursive("foo", 0, Inner(1.0)),
      TestClassRecursive("bar", 0, Inner(2.0))) should equal (Seq())
  }

  case class TestClass(a: String, b: Int, c: Double)

  val d = new CaseClassDiffy(LabelledGeneric[TestClass])

  "CaseClassDiffy" should "do a thing" in {
    d(TestClass("foo", 0, 1.0),
      TestClass("foo", 0, 1.0)) should equal (Seq())
    d(TestClass("foo", 0, 1.0),
      TestClass("bar", 0, 1.0)) should equal (Seq())
  }

}

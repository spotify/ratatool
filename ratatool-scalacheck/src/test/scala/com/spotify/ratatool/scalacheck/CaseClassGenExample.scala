/*
 * Copyright 2019 Spotify AB.
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
package com.spotify.ratatool.scalacheck

import com.spotify.ratatool.scalacheck.CaseClassGenerator._
import org.scalacheck.Gen

object CaseClassGenExampleSomeAlpha {
  implicit def alphaStrImplicit: Gen[String] = Gen.alphaLowerStr
  import OptionGen._

  val caseClassGen = deriveGen[Example]
}

object CaseClassGenExampleNumeric {
  implicit def numStrImplicit: Gen[String] = Gen.numStr

  val caseClassGen = deriveGen[Example]
}
// example implicit you might want in scope
object OptionGen {
  implicit def optionAlwaysSome[T](implicit genT: Gen[T]): Gen[Option[T]] = genT.map(Some(_))
}

// case classes to test with
case class Example(inner: Inner, s: String)
case class Inner(opt: Option[String], s: String)

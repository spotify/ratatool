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

import org.scalacheck.Prop.{AnyOperators, forAll}
import org.scalacheck.Properties

class CaseClassGenExampleTest extends Properties("CaseClassGenerator") {

  val az = "[a-z]*"
  val num = "[0-9]*"

  property("alpha some strings always alpha") = forAll(CaseClassGenExampleSomeAlpha.caseClassGen) {
    g => {
      g.s.matches(az) ?= true
      g.inner.s.matches(az) ?= true
      g.inner.opt.isDefined ?= true
      g.inner.opt.get.matches(az) ?= true
    }
  }

  property("numeric strings never alpha") = forAll(CaseClassGenExampleNumeric.caseClassGen) {
    g => {
      g.s.matches(az) ?= false
      g.inner.s.matches(az) ?= false
      g.inner.opt.map(_.matches(az) ?= false)
      g.s.matches(num) ?= true
    }
  }
}

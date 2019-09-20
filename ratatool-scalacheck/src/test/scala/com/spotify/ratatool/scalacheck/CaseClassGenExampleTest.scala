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

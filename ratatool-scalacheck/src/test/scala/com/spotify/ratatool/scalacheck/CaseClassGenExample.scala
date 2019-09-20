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
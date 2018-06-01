package com.spotify.ratatool.diffy

import org.scalatest.{FlatSpec, Matchers}
import com.spotify.ratatool.implicits._
import scala.collection.JavaConverters._

class CaseClassDiffyTest extends FlatSpec with Matchers {
  case class Foo(f1: String, f2: Int, f3: Long, f4: Seq[Double], f5: Seq[String])
  case class Bar(b1: Double, b2: Foo)

  val f1 = Foo("foo1", 1, 3L, Seq(1,3), Seq("foo1"))
  val f2 = f1.copy(f1 = "foo2", f4 = Seq(1,2), f5 = Seq("foo2"))

  val b1 = Bar(1, f1)
  val b2 = Bar(2, f2)

  val dFoo = new CaseClassDiffy[Foo]
  val dBar = new CaseClassDiffy[Bar]

  val ignoreSet = Set("f2")
  val dFooWithIgnore = new CaseClassDiffy[Foo](ignore = ignoreSet)
  val dBarWithIgnore = new CaseClassDiffy[Bar](ignore = ignoreSet)

  "CaseClassDiffy" should "support primitive fields" in {
    val result = dFoo.apply(f1, f2)

    result should contain (Delta("f1", "foo1", "foo2", StringDelta(1.0)))
    result should contain (Delta("f2", 1, 1, NumericDelta(0.0)))
    result should contain (Delta("f3", 3,3, NumericDelta(0.0)))
    result should contain (Delta("f5", List("foo1").asJava, Vector("foo2").asJava, UnknownDelta))
  }

  "CaseClassDiffy" should "support nested fields" in {
    val result = dBar.apply(b1, b2)

    result should contain (Delta("b1", 1, 2, NumericDelta(1.0)))
    result should contain (Delta("b2.f1", "foo1", "foo2", StringDelta(1.0)))
    result should contain (Delta("b2.f2", 1, 1, NumericDelta(0.0)))
    result should contain (Delta("b2.f3", 3,3, NumericDelta(0.0)))
    result should contain (Delta("b2.f5", List("foo1").asJava, Vector("foo2").asJava, UnknownDelta))
  }

  "CaseClassDiffy" should "support ignore with exact match case" in {
    val result = dFooWithIgnore.apply(f1, f2)
    result.map(_.field) shouldNot contain ("f2")
  }

  "CaseClassDiffy" should "support ignore with nested field case" in {
    val result = dBarWithIgnore.apply(b1, b2)
    result.map(_.field) shouldNot contain ("f2")
  }
}

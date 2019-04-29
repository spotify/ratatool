package com.spotify.ratatool.scalacheck

import org.scalatest._
import com.spotify.ratatool.scalacheck._
import com.spotify.ratatool.avro.specific.TestRecord
import com.spotify.ratatool.proto.Schemas.{TestRecord => ProtoTestRecord}
import org.scalacheck.Arbitrary
import Assertions._

class ArbitraryTest extends FlatSpec with Matchers {
  "arbSpecificRecord" should "be found implicitly" in {
    assertCompiles("implicitly[Arbitrary[TestRecord]]")
  }

  "arbProtoBuf" should "be found implicitly" in {
    assertCompiles("implicitly[Arbitrary[ProtoTestRecord]]")
  }
}

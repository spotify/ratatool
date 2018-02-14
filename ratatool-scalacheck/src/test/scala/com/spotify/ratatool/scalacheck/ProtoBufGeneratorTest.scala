package com.spotify.ratatool.scalacheck

import com.spotify.ratatool.proto.Schemas.TestRecord
import org.scalacheck.Properties
import org.scalacheck.Prop._


object ProtoBufGeneratorTest extends Properties("ProtoBufGenerator") {
  property("round trip") = forAll(ProtoBufGeneratorOps.protoBufOf[TestRecord]) { m =>
    m == TestRecord.parseFrom(m.toByteArray)
  }

}
package com.spotify.ratatool.describe

import com.spotify.scio.values.SCollection
import com.twitter.algebird.{Semigroup => AlgebirdSemigroup}
import org.apache.avro.generic.GenericRecord

object BigDescribeAvro {

  def pipeline(input: SCollection[GenericRecord]): SCollection[Record] = {
    implicit val recordSemigroup: AlgebirdSemigroup[Record] =
      AlgebirdSemigroup.from(Record.recordSemigroup.combine)

    input
      .withName("Map")
      .map(Record.fromGenericRecord)
      .withName("Reduce")
      .sum
  }
}

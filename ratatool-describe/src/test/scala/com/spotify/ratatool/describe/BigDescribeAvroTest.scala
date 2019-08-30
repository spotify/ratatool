package com.spotify.ratatool.describe

import com.spotify.ratatool.scalacheck.avroOf
import com.spotify.scio.testing.PipelineSpec
import org.apache.avro.Schema
import org.scalacheck.Gen

class BigDescribeAvroTest extends PipelineSpec {
  val avroSchema: Schema =
    new Schema.Parser().parse(this.getClass.getResourceAsStream("/schema.avsc"))

  "BigDescribeAvro" should "accumulate results in single record" in {
    runWithContext { sc =>
      val totalRecords = 100
      val records = Gen.listOfN(totalRecords, avroOf(avroSchema)).sample.get
      val result = BigDescribeAvro.pipeline(sc.parallelize(records))

      result should haveSize(1)
      import io.circe.syntax._
      result.map(_.asJson.spaces2).map(println)
      result.map(_.count.total) should containSingleValue(totalRecords.toLong)
    }
  }
}

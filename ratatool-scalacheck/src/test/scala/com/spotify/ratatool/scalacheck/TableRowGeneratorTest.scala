package com.spotify.ratatool.scalacheck

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import org.scalacheck.Properties
import org.scalacheck.Prop._

import scala.collection.JavaConverters._

object TableRowGeneratorTest extends Properties("TableRowGenerator") {
  val schema: TableSchema =
    new TableSchema().setFields(
      List(
        new TableFieldSchema().setName("required").setType("BOOLEAN").setMode("REQUIRED"),
        new TableFieldSchema().setName("nullable").setType("FLOAT").setMode("NULLABLE"),
        new TableFieldSchema().setName("repeated_record").setType("RECORD").setMode("REPEATED")
            .setFields(List(
              new TableFieldSchema().setName("string").setType("STRING").setMode("REQUIRED"),
              new TableFieldSchema().setName("timestamp").setType("TIMESTAMP").setMode("REQUIRED"),
              new TableFieldSchema().setName("datetime").setType("DATETIME").setMode("REQUIRED"),
              new TableFieldSchema().setName("time").setType("TIME").setMode("NULLABLE"),
              new TableFieldSchema().setName("date").setType("DATE").setMode("REQUIRED")
            ).asJava)
      ).asJava)

  property("round trip") = forAll(TableRowGeneratorOps.tableRowOf(schema)) { m =>
    m.setF(m.getF) == m
  }

}

/*
 * Copyright 2016 Spotify AB.
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

import com.google.api.services.bigquery.model.TableRow
import com.google.cloud.dataflow.sdk.coders.TableRowJsonCoder
import com.google.cloud.dataflow.sdk.util.CoderUtils
import com.spotify.ratatool.Schemas
import org.scalacheck.Prop.{BooleanOperators, all, forAll}
import org.scalacheck._

object TableRowGenSpec extends Properties("TableRowGen") {

  import TableRowGen._

  val coder = TableRowJsonCoder.of()

  property("round trips") = forAll (TableRowGen.tableRowOf(Schemas.tableSchema)) { m =>
    val base64a = CoderUtils.encodeToBase64(coder, m)
    val base64b = CoderUtils.encodeToBase64(coder, CoderUtils.decodeFromBase64(coder, base64a))
    base64a == base64b
  }

  val n = "nullable_fields"
  val richGen = tableRowOf(Schemas.tableSchema)
    .amend(Gen.choose(10L, 20L)) { r =>
      v => r.get(n).asInstanceOf[TableRow].set("int_field", v)
    }
    .amend(Gen.choose(10.0, 20.0)) { r =>
      v => r.get(n).asInstanceOf[TableRow].set("float_field", v)
    }
    .amend(Gen.const(true)) { r =>
      v => r.get(n).asInstanceOf[TableRow].set("boolean_field", v)
    }
    .amend(Gen.const("hello")) { r =>
      v => r.get(n).asInstanceOf[TableRow].set("string_field", v)
    }

  property("support RichTableRowGen") = forAll (richGen) { r =>
    val fields = r.get(n).asInstanceOf[TableRow]
    val i = fields.get("int_field").asInstanceOf[Long]
    val f = fields.get("float_field").asInstanceOf[Double]
    val b = fields.get("boolean_field").asInstanceOf[Boolean]
    val s = fields.get("string_field").asInstanceOf[String]
    all(
      "Int"     |: i >= 10L && i <= 20L,
      "Float"   |: f >= 10.0 && f <= 20.0,
      "Boolean" |: b == true,
      "String"  |: s == "hello"
    )
  }

}

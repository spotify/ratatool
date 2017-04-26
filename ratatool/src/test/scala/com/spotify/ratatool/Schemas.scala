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

package com.spotify.ratatool

import com.google.api.client.json.JsonObjectParser
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.model.TableSchema
import com.google.common.base.Charsets
import org.apache.avro.Schema

object Schemas {

  val avroSchema = new Schema.Parser().parse(this.getClass.getResourceAsStream("/schema.avsc"))

  val tableSchema = new JsonObjectParser(new JacksonFactory)
    .parseAndClose(
      this.getClass.getResourceAsStream("/schema.json"),
      Charsets.UTF_8,
      classOf[TableSchema])

}

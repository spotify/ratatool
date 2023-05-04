/*
 * Copyright 2017 Spotify AB.
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

package com.spotify.ratatool.io

import com.spotify.ratatool.avro.specific.TestRecord
import org.apache.avro.Conversions
import org.apache.avro.specific.SpecificData

import java.util.{HashMap => JHashMap, Map => JMap}
import scala.jdk.CollectionConverters._

object FixRandomData {

  /** Fix equality for maps by converting Utf8 to String */
  def apply(x: TestRecord): TestRecord = {
    def fixHashMap[T](xs: JMap[CharSequence, T]): JMap[CharSequence, T] = {
      if (xs == null) {
        null
      } else {
        val copy = new JHashMap[CharSequence, T]()
        xs.entrySet().asScala.foreach(x => copy.put(x.getKey.toString, x.getValue))
        copy
      }
    }

    // bug in avro 1.8.2. model's data is not initialized with conversions
    // force decimal conversion on the default SpecificData instance
    SpecificData.get().addLogicalTypeConversion(new Conversions.DecimalConversion())
    val newInstance = TestRecord.newBuilder(x).build()

    newInstance.getRequiredFields.setMapField(fixHashMap(x.getRequiredFields.getMapField))
    newInstance.getNullableFields.setMapField(fixHashMap(x.getNullableFields.getMapField))

    Option(newInstance.getNullableNestedField).foreach { x =>
      x.setMapField(fixHashMap(x.getMapField))
    }

    newInstance.getRepeatedNestedField.asScala.foreach { x =>
      x.setMapField(fixHashMap(x.getMapField))
    }

    newInstance
  }
}

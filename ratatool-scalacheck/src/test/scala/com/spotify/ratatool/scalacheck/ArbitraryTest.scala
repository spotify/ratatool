/*
 * Copyright 2019 Spotify AB.
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

import org.scalatest._
import com.spotify.ratatool.scalacheck._
import com.spotify.ratatool.avro.specific.TestRecord
import com.spotify.ratatool.proto.Schemas.{TestRecord => ProtoTestRecord}
import org.scalacheck.Arbitrary
import Assertions._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ArbitraryTest extends AnyFlatSpec with Matchers {
  "arbSpecificRecord" should "be found implicitly" in {
    assertCompiles("implicitly[Arbitrary[TestRecord]]")
  }

  "arbProtoBuf" should "be found implicitly" in {
    assertCompiles("implicitly[Arbitrary[ProtoTestRecord]]")
  }
}

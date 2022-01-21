/*
 * Copyright 2022 Spotify AB.
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

import org.scalacheck.Gen
import org.scalacheck.rng.Seed
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GenTestUtilsTest extends AnyFlatSpec with Matchers with GenTestUtils {
  "GenTestUtils.withGen" should "generate a test case and have an optional return value" in {
    val x = withGen(Gen.const(1))(i => i)
    assert(x.contains(1))
  }

  it should "return None on test case generation failure, not throw" in {
    val x = withGen(Gen.const(1).suchThat(_ => false))(_ => ())
    assert(x.isEmpty)
  }

  it should "propagate errors" in {
    assertThrows[RuntimeException] {
      withGen(Gen.const(1)) { _ =>
        throw new RuntimeException("xxx")
      }
    }
  }

  it should "accept a static seed" in {
    val seed = Seed.random()
    val x = withGen(Gen.choose(1, 10), seed)(i => i)
    val y = withGen(Gen.choose(1, 10), seed)(i => i)
    assert(x == y)
  }

  it should "accept a base64 string seed" in {
    val seed = Seed.random().toBase64
    val x = withGen(Gen.choose(1, 10), seed)(i => i)
    val y = withGen(Gen.choose(1, 10), seed)(i => i)
    assert(x == y)
  }
}

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

package com.spotify.ratatool.testing

import com.google.cloud.dataflow.sdk.testing.DataflowAssert
import com.google.cloud.dataflow.sdk.util.CoderUtils
import com.google.protobuf.GeneratedMessage
import com.spotify.ratatool.diffy.{Diffy, ProtoBufDiffy}
import com.spotify.scio.values.SCollection
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.control.NonFatal

/** Trait with ScalaTest [[Matcher]]s for [[SCollection]]s that leverages [[Diffy]]. */
trait DiffyMatchers {

  private def m(f: () => Any): MatchResult = {
    val r = try { f(); true } catch { case NonFatal(_) => false }
    MatchResult(r, "", "")
  }

  // Due to  https://github.com/GoogleCloudPlatform/DataflowJavaSDK/issues/434
  // SerDe cycle on each element to keep consistent with values on the expected side
  private def serDeCycle[T: ClassTag](scollection: SCollection[T]): SCollection[T] = {
    val coder = scollection.internal.getCoder
    scollection
      .map(e => CoderUtils.decodeFromByteArray(coder, CoderUtils.encodeToByteArray(coder, e)))
  }

  class Diffyable[T <: GeneratedMessage : ClassTag](val value: T, val unordered: Set[String])
    extends Serializable {
    override def equals(that: Any): Boolean = that match {
      case d: Diffyable[T] =>
        new ProtoBufDiffy[T](unordered = unordered).apply(this.value, d.value).isEmpty
      case _ => false
    }
    override def hashCode(): Int = value.hashCode()
  }

  def beEquivalentInAnyOrder[T <: GeneratedMessage : ClassTag]
  (unordered: Set[String])(value: Iterable[T])
  : Matcher[SCollection[T]] = new Matcher[SCollection[T]] {
    override def apply(left: SCollection[T]): MatchResult = {
      val leftD = left.map(new Diffyable(_, unordered))
      val rightD = value.map(new Diffyable(_, unordered))
      m(() => DataflowAssert.that(serDeCycle(leftD).internal).containsInAnyOrder(rightD.asJava))
    }
  }

}

/*
 * Copyright 2023 Spotify AB.
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

import org.scalacheck.util.Buildable

private[scalacheck] object HashMapBuildable {

  import scala.jdk.CollectionConverters._

  implicit val tt: java.util.HashMap[CharSequence, Any] => Traversable[(CharSequence, Any)] =
    _.asScala
  implicit def buildableHashMap[K, V]: Buildable[(K, V), java.util.HashMap[K, V]] =
    new Buildable[(K, V), java.util.HashMap[K, V]] {
      def builder = new HashMapBuilder[K, V]
    }

}

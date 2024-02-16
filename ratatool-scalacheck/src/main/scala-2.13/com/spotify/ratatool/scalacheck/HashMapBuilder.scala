/*
 * Copyright 2024 Spotify AB.
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

import scala.collection.mutable.Builder

private[scalacheck] class HashMapBuilder[K, V] extends Builder[(K, V), java.util.HashMap[K, V]] {
  private val hm = new java.util.HashMap[K, V]
  def addOne(x: (K, V)): this.type = {
    hm.put(x._1, x._2)
    this
  }
  def clear(): Unit = hm.clear()
  def result(): java.util.HashMap[K, V] = hm
}

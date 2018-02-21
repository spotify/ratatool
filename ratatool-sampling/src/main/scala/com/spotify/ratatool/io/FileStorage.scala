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

package com.spotify.ratatool.io

import java.io.FileNotFoundException
import java.net.URI
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel

import org.apache.avro.file.SeekableInput
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.io.fs.MatchResult.Metadata

import scala.collection.JavaConverters._

private[ratatool] object FileStorage {
  def apply(path: String): FileStorage = new FileStorage(path)
  def isLocalUri(uri: URI): Boolean = uri.getScheme == null || uri.getScheme == "file"
  def isGcsUri(uri: URI): Boolean = uri.getScheme == "gs"
  def isHdfsUri(uri: URI): Boolean = uri.getScheme == "hdfs"
}

private[ratatool] class FileStorage(protected[io] val path: String) {

  def exists: Boolean = ! FileSystems.`match`(path).metadata.isEmpty

  def listFiles: Seq[Metadata] = FileSystems.`match`(path).metadata().asScala

  def isDone: Boolean = {
    val partPattern = "([0-9]{5})-of-([0-9]{5})".r
    val metadata = try {
      listFiles
    } catch {
      case e: FileNotFoundException => Seq.empty
    }
    val nums = metadata.flatMap { meta =>
      val m = partPattern.findAllIn(meta.resourceId().toString)
      if (m.hasNext) {
        Some(m.group(1).toInt, m.group(2).toInt)
      } else {
        None
      }
    }

    if (metadata.isEmpty) {
      // empty list
      false
    } else if (nums.nonEmpty) {
      // found xxxxx-of-yyyyy pattern
      val parts = nums.map(_._1).sorted
      val total = nums.map(_._2).toSet
      metadata.size == nums.size &&  // all paths matched
        total.size == 1 && total.head == parts.size &&  // yyyyy part
        parts.head == 0 && parts.last + 1 == parts.size // xxxxx part
    } else {
      true
    }
  }

}

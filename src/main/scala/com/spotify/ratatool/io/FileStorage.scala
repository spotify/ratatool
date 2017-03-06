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

import java.io._
import java.net.URI
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.file.{Files, Path}
import java.util.Collections
import java.util.regex.Pattern

import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory
import com.google.cloud.dataflow.sdk.util.GcsUtil.GcsUtilFactory
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath
import org.apache.avro.file.{SeekableFileInput, SeekableInput}
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.WildcardFileFilter

import scala.collection.JavaConverters._
import scala.util.Try

private[ratatool] object FileStorage {
  def apply(path: String): FileStorage =
    if (isGcsUri(new URI(path))) new GcsStorage(path) else new LocalStorage(path)
  def isLocalUri(uri: URI): Boolean = uri.getScheme == null || uri.getScheme == "file"
  def isGcsUri(uri: URI): Boolean = uri.getScheme == "gs"
  def isHdfsUri(uri: URI): Boolean = uri.getScheme == "hdfs"
}

private[ratatool] trait FileStorage {

  protected val path: String

  protected def listFiles: Seq[Path]

  protected def getObjectInputStream(path: Path): InputStream

  def getAvroSeekableInput: SeekableInput

  def exists: Boolean

  def isDone: Boolean = {
    val partPattern = "([0-9]{5})-of-([0-9]{5})".r
    val paths = listFiles
    val nums = paths.flatMap { p =>
      val m = partPattern.findAllIn(p.toString)
      if (m.hasNext) {
        Some(m.group(1).toInt, m.group(2).toInt)
      } else {
        None
      }
    }

    if (paths.isEmpty) {
      // empty list
      false
    } else if (nums.nonEmpty) {
      // found xxxxx-of-yyyyy pattern
      val parts = nums.map(_._1).sorted
      val total = nums.map(_._2).toSet
      paths.size == nums.size &&  // all paths matched
        total.size == 1 && total.head == parts.size &&  // yyyyy part
        parts.head == 0 && parts.last + 1 == parts.size // xxxxx part
    } else {
      true
    }
  }

  private def getDirectoryInputStream(path: String,
                                      wrapperFn: InputStream => InputStream = identity)
  : InputStream = {
    val inputs = listFiles.map(getObjectInputStream).map(wrapperFn).asJava
    new SequenceInputStream(Collections.enumeration(inputs))
  }

}

private class GcsStorage(protected val path: String) extends FileStorage {

  private val uri = new URI(path)
  require(FileStorage.isGcsUri(uri), s"Not a GCS path: $path")

  private lazy val gcs = new GcsUtilFactory().create(PipelineOptionsFactory.create())

  private val GLOB_PREFIX = Pattern.compile("(?<PREFIX>[^\\[*?]*)[\\[*?].*")

  override protected def listFiles: Seq[Path] = {
    if (GLOB_PREFIX.matcher(path).matches()) {
      gcs
        .expand(GcsPath.fromUri(uri))
        .asScala
    } else {
      // not a glob, GcsUtil may return non-existent files
      val p = GcsPath.fromUri(path)
      if (Try(gcs.fileSize(p)).isSuccess) Seq(p) else Seq.empty
    }
  }

  override protected def getObjectInputStream(path: Path): InputStream =
    Channels.newInputStream(gcs.open(GcsPath.fromUri(path.toUri)))

  override def getAvroSeekableInput: SeekableInput =
    new SeekableInput {
      private val in = gcs.open(GcsPath.fromUri(path))

      override def tell(): Long = in.position()

      override def length(): Long = in.size()

      override def seek(p: Long): Unit = in.position(p)

      override def read(b: Array[Byte], off: Int, len: Int): Int =
        in.read(ByteBuffer.wrap(b, off, len))

      override def close(): Unit = in.close()
    }

  override def exists: Boolean = {
    try {
      if (GLOB_PREFIX.matcher(path).matches()) {
        gcs
          .expand(GcsPath.fromUri(uri))
          .asScala.nonEmpty
      } else {
        gcs.fileSize(GcsPath.fromUri(path))
        true
      }
    } catch {
      case _: FileNotFoundException => false
    }
  }
}

private class LocalStorage(protected val path: String)  extends FileStorage {

  private val uri = new URI(path)
  require(FileStorage.isLocalUri(uri), s"Not a local path: $path")

  override protected def listFiles: Seq[Path] = {
    val p = path.lastIndexOf("/")
    val (dir, filter) = if (p == 0) {
      // "/file.ext"
      (new File("/"), new WildcardFileFilter(path.substring(p + 1)))
    } else if (p > 0) {
      // "/path/to/file.ext"
      (new File(path.substring(0, p)), new WildcardFileFilter(path.substring(p + 1)))
    } else {
      // "file.ext"
      (new File("."), new WildcardFileFilter(path))
    }

    if (dir.exists()) {
      FileUtils
        .listFiles(dir, filter, null)
        .asScala
        .toSeq
        .map(_.toPath)
    } else {
      Seq.empty
    }
  }

  override protected def getObjectInputStream(path: Path): InputStream =
    new FileInputStream(path.toFile)

  override def getAvroSeekableInput: SeekableInput =
    new SeekableFileInput(new File(path))

  override def exists: Boolean = listFiles.nonEmpty
}

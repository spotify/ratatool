package com.spotify.ratatool.io

import java.io.{File, InputStream, OutputStream}
import java.nio.file.Files

import com.spotify.ratatool.GcsConfiguration
import org.apache.avro.Schema
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter}

/**
 * Utilities for Parquet IO.
 *
 * Records are represented as Avro records.
 */
object ParquetIO {

  /** Read records from a file. */
  def readFromFile[T](path: Path): Iterator[T] = {
    val conf = GcsConfiguration.get()
    val reader = AvroParquetReader.builder[T](path)
      .withConf(conf)
      .build()

    new Iterator[T] {
      private var item = reader.read()
      override def hasNext: Boolean = item != null
      override def next(): T = {
        val r = item
        item = reader.read()
        r
      }
    }
  }

  /** Read records from a file. */
  def readFromFile[T](name: String): Iterator[T] = readFromFile(new Path(name))

  /** Read records from a file. */
  def readFromFile[T](file: File): Iterator[T] = readFromFile(file.getAbsolutePath)

  /** Read records from an [[InputStream]]. */
  def readFromInputStream[T](is: InputStream): Iterator[T] = {
    val dir = Files.createTempDirectory("ratatool-")
    val file = new File(dir.toString, "temp.parquet")
    Files.copy(is, file.toPath)
    val data = readFromFile(file)
    FileUtils.deleteDirectory(dir.toFile)
    data
  }

  /** Read records from a resource file. */
  def readFromResource[T](name: String): Iterator[T] =
    readFromInputStream(this.getClass.getResourceAsStream(name))

  /** Write records to a file. */
  def writeToFile[T](data: Iterable[T], schema: Schema, path: Path): Unit = {
    val conf = GcsConfiguration.get()
    val writer = AvroParquetWriter.builder[T](path)
      .withConf(conf)
      .withSchema(schema)
      .build()
    data.foreach(writer.write)
    writer.close()
  }

  /** Write records to a file. */
  def writeToFile[T](data: Iterable[T], schema: Schema, name: String): Unit =
    writeToFile(data, schema, new Path(name))

  /** Write records to a file. */
  def writeToFile[T](data: Iterable[T], schema: Schema, file: File): Unit =
    writeToFile(data, schema, file.getAbsolutePath)

  /** Write records to an [[OutputStream]]. */
  def writeToOutputStream[T](data: Iterable[T], schema: Schema, os: OutputStream): Unit = {
    val dir = Files.createTempDirectory("ratatool-")
    val file = new File(dir.toString, "temp.parquet")
    writeToFile(data, schema, file)
    Files.copy(file.toPath, os)
    FileUtils.deleteDirectory(dir.toFile)
  }

}

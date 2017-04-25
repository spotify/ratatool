Ratatool
========

[![Build Status](https://travis-ci.org/spotify/ratatool.svg?branch=master)](https://travis-ci.org/spotify/ratatool)
[![codecov.io](https://codecov.io/github/spotify/ratatool/coverage.svg?branch=master)](https://codecov.io/github/spotify/ratatool?branch=master)
[![GitHub license](https://img.shields.io/github/license/spotify/ratatool.svg)](./LICENSE)
[![Maven Central](https://img.shields.io/maven-central/v/com.spotify/ratatool_2.11.svg)](https://maven-badges.herokuapp.com/maven-central/com.spotify/ratatool_2.11)

A tool for random data sampling and generation

# Features

- [Generators](./src/main/scala/com/spotify/ratatool/generators) - random data generators for [Avro](https://avro.apache.org/), [Protocol Buffers](https://developers.google.com/protocol-buffers/) and [BigQuery](https://cloud.google.com/bigquery/) [TableRow](https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/java/latest/com/google/api/services/bigquery/model/TableRow.html)
- [IO](./src/main/scala/com/spotify/ratatool/io) - utilities for reading and writing records in Avro, [Parquet](http://parquet.apache.org/) (via Avro GenericRecord), BigQuery and TableRow JSON files. Local file system, HDFS and [Google Cloud Storage](https://cloud.google.com/storage/) are supported.
- [Samplers](./src/main/scala/com/spotify/ratatool/samplers) - random data samplers for Avro, BigQuery and Parquet. True random sampling is supported for Avro only while head mode (sampling from the start) is supported for all sources.
- [ScalaCheck](./src/main/scala/com/spotify/ratatool/scalacheck) - [ScalaCheck](http://scalacheck.org/) generators (`Gen[T]`) for property-based testing.
- [Diffy](./src/main/scala/com/spotify/ratatool/diffy) - field-level record diff tool for Avro, Protobuf and BigQuery TableRow.
- [BigDiffy](./src/main/scala/com/spotify/ratatool/diffy/BigDiffy.scala) - [Scio](https://github.com/spotify/scio) library for pairwise field-level statistical diff of data sets. See [slides](http://www.lyh.me/slides/bigdiffy.html) for more.
- [Command line tool](./src/main/scala/com/spotify/ratatool/tool) - command line tool for sampling from various sources.

# Usage

If you use [sbt](http://www.scala-sbt.org/) add the following dependency to your build file:
```scala
libraryDependencies += "com.spotify" %% "ratatool" % "0.1.9" % "test"
```

Or install via our [Homebrew tap](https://github.com/spotify/homebrew-public) if you're on a Mac:

```
brew tap spotify/public
brew install ratatool
```

Or download the [release](https://github.com/spotify/ratatool/releases) jar and run it.

```bash
wget https://github.com/spotify/ratatool/releases/download/v0.1.9/ratatool-0.1.9.jar
java -jar ratatool-0.1.9.jar
```

The command line tool can be used to sample from local file system or Google Cloud Storage directly if [Google Cloud SDK](https://cloud.google.com/sdk/) is installed and authenticated.

```bash
java -jar ratatool-0.1.4.jar avro --head -n 1000 --in gs://path/to/dataset --out out.avro
java -jar ratatool-0.1.4.jar parquet --head -n 1000 --in gs://path/to/dataset --out out.parquet

# write output to both JSON file and BigQuery table
java -jar ratatool-0.1.4.jar bigquery --head -n 1000 --in project_id:dataset_id.table_id \
    --out out.json--tableOut project_id:dataset_id.table_id
```

It can also be used to sample from HDFS with if `core-site.xml` and `hdfs-site.xml` are available.

```bash
java -cp ratatool-0.1.4.jar:/path/to/hadoop/conf com.spotify.ratatool.tool.Tool avro \
    --head -n 10 --in hdfs://namenode/path/to/dataset --out file:///path/to/out.avro
```

# License

Copyright 2016-2017 Spotify AB.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

Ratatool
========

[![Build Status](https://travis-ci.org/spotify/ratatool.svg?branch=master)](https://travis-ci.org/spotify/ratatool)
[![codecov.io](https://codecov.io/github/spotify/ratatool/coverage.svg?branch=master)](https://codecov.io/github/spotify/ratatool?branch=master)
[![GitHub license](https://img.shields.io/github/license/spotify/ratatool.svg)](./LICENSE)
[![Maven Central](https://img.shields.io/maven-central/v/com.spotify/ratatool-common_2.12.svg)](https://maven-badges.herokuapp.com/maven-central/com.spotify/ratatool-common_2.12)

A tool for random data sampling and generation

# Features

- [ScalaCheck Generators](https://github.com/spotify/ratatool/tree/master/ratatool-scalacheck) - [ScalaCheck](http://scalacheck.org/) generators (`Gen[T]`) for property-based testing for [Avro](https://avro.apache.org/), [Protocol Buffers](https://developers.google.com/protocol-buffers/) and [BigQuery](https://cloud.google.com/bigquery/) [TableRow](https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/java/latest/com/google/api/services/bigquery/model/TableRow.html)
- [IO](https://github.com/spotify/ratatool/tree/master/ratatool-sampling/src/main/scala/com/spotify/ratatool/io) - utilities for reading and writing records in Avro, [Parquet](http://parquet.apache.org/) (via Avro GenericRecord), BigQuery and TableRow JSON files. Local file system, HDFS and [Google Cloud Storage](https://cloud.google.com/storage/) are supported.
- [Samplers](https://github.com/spotify/ratatool/tree/master/ratatool-sampling) - random data samplers for Avro, BigQuery and Parquet. True random sampling is supported for Avro only while head mode (sampling from the start) is supported for all sources.
- [Diffy](https://github.com/spotify/ratatool/tree/master/ratatool-diffy) - field-level record diff tool for Avro, Protobuf and BigQuery TableRow.
- [BigDiffy](https://github.com/spotify/ratatool/blob/master/ratatool-diffy) - [Scio](https://github.com/spotify/scio) library for pairwise field-level statistical diff of data sets. See [slides](http://www.lyh.me/slides/bigdiffy.html) for more.
- [Command line tool](https://github.com/spotify/ratatool/tree/master/ratatool-cli/src/main/scala/com/spotify/ratatool/tool) - command line tool for local sampler, or executing BigDiffy and BigSampler.
- [Shapeless](https://github.com/spotify/ratatool/tree/master/ratatool-shapeless) - An extension for Case Class Diffing via Shapeless.

For more information or documentation, project level READMEs are provided.

# Usage

If you use [sbt](http://www.scala-sbt.org/) add the following dependency to your build file:
```scala
libraryDependencies += "com.spotify" %% "ratatool-scalacheck" % "0.3.10" % "test"
```

If needed, the following other libraries are published:
* `ratatool-diffy`
* `ratatool-sampling`

Or install via our [Homebrew tap](https://github.com/spotify/homebrew-public) if you're on a Mac:

```
brew tap spotify/public
brew install ratatool
ratatool
```

Or download the [release](https://github.com/spotify/ratatool/releases) jar and run it.

```bash
wget https://github.com/spotify/ratatool/releases/download/v0.3.10/ratatool-cli-0.3.10.tar.gz
bin/ratatool directSampler
```

The command line tool can be used to sample from local file system or Google Cloud Storage directly if [Google Cloud SDK](https://cloud.google.com/sdk/) is installed and authenticated.

```bash
bin/ratatool bigSampler avro --head -n 1000 --in gs://path/to/dataset --out out.avro
bin/ratatool bigSampler parquet --head -n 1000 --in gs://path/to/dataset --out out.parquet

# write output to both JSON file and BigQuery table
bin/ratatool bigSampler bigquery --head -n 1000 --in project_id:dataset_id.table_id \
    --out out.json--tableOut project_id:dataset_id.table_id
```

It can also be used to sample from HDFS with if `core-site.xml` and `hdfs-site.xml` are available.

```bash
bin/ratatool bigSampler avro \
    --head -n 10 --in hdfs://namenode/path/to/dataset --out file:///path/to/out.avro
```

Or execute BigDiffy directly

```bash
bin/ratatool bigDiffy \
    --input_mode=avro \
    --key=record.key \
    --lhs=gs://path/to/left \
    --rhs=gs://path/to/right \
    --output=gs://path/to/output \
    --runner=DataflowRunner ....
```

# License

Copyright 2016-2018 Spotify AB.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

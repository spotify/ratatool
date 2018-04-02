Sampler
=======

Diffy contains record sampling classes for Avro, BigQuery, Parquet.

# BigSampler

BigSampler will run a [Scio](https://github.com/spotify/scio) pipeline sampling either Avro or BigQuery data.
 It also allows specifying a seed and fields to hash for deterministic cohort selection.

For full details see [BigSample.scala](https://github.com/spotify/ratatool/blob/master/ratatool-sampling/src/main/scala/com/spotify/ratatool/samplers/BigSampler.scala)

Supported filesystems include Local, GCS (`gs://`), and HDFS (`hdfs://`).

## Usage

```
BigSampler
Usage: BigSampler [dataflow_options] [options]

  --sample=<percentage>             Percentage of records to take in sample, a decimal between 0.0 and 1.0
  --input=<path>                    Input file path or BigQuery table
  --output=<path>                   Output file path or BigQuery table
  [--fields=<field1,field2,...>]    An optional list of fields to include in hashing for sampling cohort selection
  [--seed=<seed]                    An optional seed using in hashing for sampling cohort selection
```

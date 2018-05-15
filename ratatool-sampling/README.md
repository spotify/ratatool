Sampler
=======

Diffy contains record sampling classes for Avro, BigQuery, Parquet. Supported filesystems include Local, GCS (`gs://`), and HDFS (`hdfs://`).

# BigSampler

BigSampler will run a [Scio](https://github.com/spotify/scio) pipeline sampling either Avro or BigQuery data.
 It also allows specifying a hash function (either FarmHash or Murmur) with seed (if applicable for 
 your hash) and fields to hash for deterministic cohort selection.

For full details see [BigSample.scala](https://github.com/spotify/ratatool/blob/master/ratatool-sampling/src/main/scala/com/spotify/ratatool/samplers/BigSampler.scala)

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

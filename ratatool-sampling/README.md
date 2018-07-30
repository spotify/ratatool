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
BigSampler - a tool for big data sampling
Usage: ratatool $command [dataflow_options] [options]

  --sample=<percentage>                      Percentage of records to take in sample, a decimal between 0.0 and 1.0
  --input=<path>                             Input file path or BigQuery table
  --output=<path>                            Output file path or BigQuery table
  [--fields=<field1,field2,...>]             An optional list of fields to include in hashing for sampling cohort selection
  [--seed=<seed>]                            An optional seed used in hashing for sampling cohort selection
  [--distribution=(uniform|stratified)]      An optional arg to sample for a stratified or uniform distribution. Must provide `distributionFields`
  [--distributionFields=<field1,field2,...>] An optional list of fields to sample for distribution. Must provide `distribution`
  [--exact]                                  An optional arg for higher precision distribution sampling. By default the sampling is approximate.
```

### Reproducible Sampling
Leveraging `--fields=<field1,field2,...>` BigSampler can produce a hash based on the specified
 fields. This ensures that rows will always produce the same value to determine whether or not they
 are in the sample. For example, `--fields=user_id --sample=0.5` will always produce the same sample
 of 50% of users. If multiple records contain the same `user_id` they will all be in or out of the
 sample.
 
## Sampling a Distribution
BigSampler supports sampling to produce either a Stratified or Uniform distribution.
```
  [--distribution=(uniform|stratified)]      An optional arg to sample for a stratified or uniform distribution. Must provide `distributionFields`
  [--distributionFields=<field1,field2,...>] An optional list of fields to sample for distribution. Must provide `distribution`
  [--exact]                                  An optional arg for higher precision distribution sampling.
``` 

As an example, `--distribution=stratified --distributionFields=country --sample=0.25` produces
 an output 25% the size of the original dataset that preserves the existing distribution across all
 strata (all countries in this case).
 
`--exact` attempts to more precisely match the distribution but is more exhaustive and therefore
 less performant, and may have issues dealing with very large datasets.
 
BigSampler will also output metrics in logs for how close it came to the target sample populations
 per strata, and in `--exact` mode it will error if the produced sample misses by too much.

Distribution sampling currently does *not* support sampling with replacement.
Distribution sampling currently assumes all distinct keys or strata can fit into memory (this allows
 leveraging `hashJoin` for performance improvements)
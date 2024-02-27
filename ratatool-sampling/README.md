Sampler
=======

Diffy contains record sampling classes for Avro, Parquet, and BigQuery. Supported filesystems include Local, GCS (`gs://`), and HDFS (`hdfs://`).

# BigSampler

BigSampler will run a [Scio](https://github.com/spotify/scio) pipeline sampling either Avro or BigQuery data.
 It also allows specifying a hash function (either [FarmHash](https://github.com/google/farmhash) or Murmur) with seed (if applicable for 
 your hash) and fields to hash for deterministic cohort selection.

For full details see [BigSample.scala](https://github.com/spotify/ratatool/blob/master/ratatool-sampling/src/main/scala/com/spotify/ratatool/samplers/BigSampler.scala)

## Usage

### Command Line
```
BigSampler - a tool for big data sampling
Usage: ratatool bigSampler [dataflow_options] [options]

  --sample=<percentage>                               Percentage of records to take in sample, a decimal between 0.0 and 1.0
  --input=<path>                                      Input file path or BigQuery table
  --output=<path>                                     Output file path or BigQuery table
  [--fields=<field1,field2,...>]                      An optional list of fields to include in hashing for sampling cohort selection
  [--seed=<seed>]                                     An optional seed used in hashing for sampling cohort selection
  [--hashAlgorithm=(murmur|farm)]                     An optional arg to select the hashing algorithm for sampling cohort selection. Defaults to FarmHash for BigQuery compatibility
  [--distribution=(uniform|stratified)]               An optional arg to sample for a stratified or uniform distribution. Must provide `distributionFields`
  [--distributionFields=<field1,field2,...>]          An optional list of fields to sample for distribution. Must provide `distribution`
  [--exact]                                           An optional arg for higher precision distribution sampling.
  [--bigqueryPartitioning=<day|hour|month|year|null>] An optional arg specifying what partitioning to use for the output BigQuery table, or 'null' for no partitioning. Defaults to day.

Since this runs a Scio/Beam pipeline, Dataflow options will have to be provided. At a
minimum, the following should be specified:

   --project=<gcp-project-id>                GCP Project used to run your job
   --runner=DataflowRunner                   Executes the job on Google Cloud Dataflow
   --tempLocation=<gcs-path>                 Location for temporary files. GCS bucket must be created prior to running job.


The following options are recommended, but may not be necessary.

   --serviceAccount=<your-service-account>   Service account used on Dataflow workers. Useful to avoid permissions issues.
   --workerMachineType=<machine-type>        Can be tweaked based on your specific needs, but is not necessary.
   --maxNumWorkers=<num-workers>             Limits the number of workers (machines) used in the job to avoid using up quota.

For more details regarding Dataflow options see here: https://cloud.google.com/dataflow/pipelines/specifying-exec-params
```

### Importing into a Scio Pipeline
BigSampler also provides some helper functions for usage in a Scio pipeline. These are provided for
 GenericRecord, TableRow, and Protobuf Messages.
 
Here is the example for Protobuf. There is also `sampleAvro` and `sampleBigQuery` defined with
 similar signatures.
```scala
/**
* Sample wrapper function for Avro GenericRecord
* @param coll The input SCollection to be sampled
* @param fraction The sample rate
* @param fields Fields to construct hash over for determinism
* @param seed Seed used to salt the deterministic hash
* @param hashAlgorithm Hashing algorithm to use
* @param distribution Desired output sample distribution
* @param distributionFields Fields to construct distribution over (strata = set of unique fields)
* @param precision Approximate or Exact precision
* @param maxKeySize Maximum allowed size per key (can be tweaked for very large data sets)
* @tparam T Record Type
* @return SCollection containing Sample population
*/
def sampleProto[T <: AbstractMessage : ClassTag](coll: SCollection[T],
                                                fraction: Double,
                                                fields: Seq[String] = Seq(),
                                                seed: Option[Int] = None,
                                                hashAlgorithm: HashAlgorithm = FarmHash,
                                                distribution: Option[SampleDistribution]=None,
                                                distributionFields: Seq[String] = Seq(),
                                                precision: Precision = Approximate,
                                                maxKeySize: Int = 1e6.toInt)
: SCollection[T]
```
For an example of usage, see [ProtoSamplerExample.scala](https://github.com/spotify/ratatool/blob/master/ratatool-examples/src/main/scala/com/spotify/ratatool/examples/samplers/ProtoSamplerExample.scala)

## Reproducible Sampling
Leveraging `--fields=<field1,field2,...>` BigSampler can produce a hash based on the specified
 fields. This ensures that rows will always produce the same value to determine whether or not they
 are in the sample. For example, `--fields=user_id --sample=0.5` will always produce the same sample
 of 50% of users. If multiple records contain the same `user_id` they will all be in or out of the
 sample.

### Reproducing within BigQuery
Currently, BigSampler defaults to Farmhash, which is also used in BigQuery. When sampling with a seed and one or more fields,
 under the hood Farmhash will create a byte array, convert all inputs to bytes, and concatenate them together. To recreate this in BigQuery, you
 will have to pre-create the seed as a little endian hex encoded byte string, as BigQuery does not currently allow directly converting an integer
 to bytes.

`FARM_FINGERPRINT(CONCAT(b'\x2A\x00\x00\x00', CAST('abc' as BYTES))` will produce the equivalent hash of `--seed=42` with one `fields` where the given record has value `abc`.

The output will also need to be normalized to the range [0.0, 1.0] from the range [Long.MinValue, Long.MaxValue] in order to produce the exact equivalent sample as BigSampler.
 
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
 per strata, and in `--exact` mode it will error if the produced sample misses by `> 1%`.

Distribution sampling currently does *not* support sampling with replacement.
Distribution sampling currently assumes all distinct keys or strata can fit into memory (this allows
 leveraging `hashJoin` for performance improvements)
 
## Distributions
### Stratified
![Stratified](https://github.com/spotify/ratatool/blob/master/misc/Stratified.png)
Stratified sampling example. Note that only the specified distributionFields are preserved in the sample.

![Uniform](https://github.com/spotify/ratatool/blob/master/misc/Uniform.png)
Uniform sampling example. Adjusts input to produce an even output distribution if possible.

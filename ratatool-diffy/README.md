Diffy
=======

Diffy contains record diff-ing classes that can be utilized by BigDiffy to perform Diffs over large datasets.
 Supported filesystems include Local, GCS (`gs://`), HDFS (`hdfs://`). There is also support for diff-ing directly from BigQuery tables.
 Currently supported formats are Avro, Parquet, Protobuf, or BigQuery TableRow, but the CLI only supports Avro and BigQuery TableRow.
 
# BigDiffy

BigDiffy will run a [Scio](https://github.com/spotify/scio) pipeline diffing a LHS and RHS by key
 and output statistics based on the differences. The output contains 3 folders:

 * `global` - Global counts of Diff types (`SAME`, `DIFFERENT`, `MISSING_LHS`, `MISSING_RHS`) seen in the entire dataset (See `GlobalStats`)
 * `fields` - Field level statistics including, but not limited to, the number of records with different values per field, min, max, standard deviation (See `FieldStats` and `DeltaStats`)
 * `keys` - All unique keys found in the two datasets and their Diff types by key in pairs of `<key>\t<diffType>` (See `KeyStats`). If fields are different it will output stats for every field which is different, including left and right, as well as distance if the field is `NUMERIC`, `STRING`, or `VECTOR`

For full details on Statistics and output see [BigDiffy.scala](https://github.com/spotify/ratatool/blob/master/ratatool-diffy/src/main/scala/com/spotify/ratatool/diffy/BigDiffy.scala)


## Usage
From the CLI
```
BigDiffy - pair-wise field-level statistical diff
Usage: ratatool bigDiffy [dataflow_options] [options]

  --input-mode=(avro|bigquery|parquet)     Diff-ing Avro, Parquet or BQ records
  [--output-mode=(gcs|bigquery)]   Saves to a text file in GCS or a BigQuery dataset. Defaults to GCS
  --key=<key>                      '.' separated key field. Specify multiple --key params or multiple ',' separated key fields for multi key usage.
                                   No two (or more) records within the same dataset may have the same key,
                                   or the workflow will fail with "More than two values found for key".
  --lhs=<path>                     LHS File path or BigQuery table
  --rhs=<path>                     RHS File path or BigQuery table
  [--rowRestriction=<filter>]      SQL text filtering statement to apply to BigQuery inputs (not available for avro inputs),
                                   similar to a WHERE clause in a query. Aggregates are not supported. Defaults to None
  --output=<output>                File path prefix for output
  --ignore=<keys>                  ',' separated field list to ignore
  --unordered=<keys>               ',' separated field list to treat as unordered
  --unorderedFieldKey=<key>        ',' separated list of keys for fields which are unordered nested records. Mappings use ':'
                                   For example --unorderedFieldKey=fieldPath:fieldKey,otherPath:otherKey
  [--with-header]                  Output all TSVs with header rows. Defaults to false
  [--ignore-nan]                   Ignore NaN values when computing stats for differences

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

Or from SBT
```
libraryDependencies += "com.spotify" %% "ratatool-diffy" % ratatoolVersion

```
The latest version can be found in the main [README](https://github.com/spotify/ratatool/blob/master/README.md).

## Nested repeated records
If you need to use `unorderedFieldKeys` to diff nested repeated records, then you will have to
 write a Scala job which calls [BigDiffy](https://github.com/spotify/ratatool/blob/master/ratatool-diffy/src/main/scala/com/spotify/ratatool/diffy/BigDiffy.scala)
 as this is currently unsupported from the CLI.

## Schema Evolution

If you are diffing two avro datasets, their schemas must be backwards compatible (with the "new" schema on the RHS).

For BigQuery datasets, the diff is applied across the union of the two schemas.

You can also add the new fields to the ignore list to prune them from the diff results.


## Row Restriction

The `rowRestriction` argument for BigQuery inputs is passed directly through to BigQuery's
[Storage API](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#tablereadoptions).
Any SQL-style predicate should work, including:

- Simple filters: `--rowRestriction=field_a=1`
- Selecting partition of a natively partitioned table:
  `--rowRestriction="date_field=DATE '2022-01-01'"` (note that date values need to be explicitly cast)
- Compound filters: `--rowRestriction="field_a=1 AND field_b=3"`

The `rowRestriction` arg can be wrapped in quotes to allow for spaces in the predicate text.

`rowRestriction` is an optional argument. Not passing this argument will result in all rows being 
read from the BigQuery inputs.

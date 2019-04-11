Diffy
=======

Diffy contains record diff-ing classes that can be utilized by BigDiffy to perform Diffs over large datasets.
 Supported filesystems include Local, GCS (`gs://`), HDFS (`hdfs://`). There is also support for diff-ing directly from BigQuery tables.
 Currently supported formats are Avro, Protobuf, or BigQuery TableRow, but the CLI only supports Avro and TableRow.
 
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

  --input-mode=(avro|bigquery)     Diff-ing Avro or BQ records
  [--output-mode=(gcs|bigquery)]   Saves to a text file in GCS or a BigQuery dataset. Defaults to GCS
  --key=<key>                      '.' separated key field
  --lhs=<path>                     LHS File path or BigQuery table
  --rhs=<path>                     RHS File path or BigQuery table
  --output=<output>                File path prefix for output
  --ignore=<keys>                  ',' separated field list to ignore
  --unordered=<keys>               ',' separated field list to treat as unordered
  [--with-header]                  Output all TSVs with header rows. Defaults to false

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

## Schema Evolution
If you are trying to diff two schemas that are backwards compatible, you should put the "new" schema
 which is backwards compatible on the RHS. You can also add the new fields to the ignore list to
 prune them from the diff results. For BigQuery, the diff is applied across the union of the two
 schemas. For Avro, the RHS is used as the source of truth, and the diff is assumed to apply to all
 fields in the RHS unless it is specified in `ignore`.

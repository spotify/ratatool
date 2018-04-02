Diffy
=======

Diffy contains record diff-ing classes that can be utilized by BigDiffy to perform Diffs over large datasets. Supported filesystems include Local, GCS (`gs://`), and HDFS (`hdfs://`)

# BigDiffy

BigDiffy will run a [Scio](https://github.com/spotify/scio) pipeline diffing a LHS and RHS by key
 and output statistics based on the differences. The output contains 3 folders:

 * `global` - Global counts of Diff types (`SAME`, `DIFFERENT`, `MISSING_LHS`, `MISSING_RHS`) seen in the entire dataset
 * `fields` - Field level statistics including, but not limited to, the number of records with different values per field, min, max, standard deviation 
 * `keys` - All unique keys found in the two datasets and their Diff types by key in pairs of `<key>\t<diffType>`

For full details on Statistics and output see [BigDiffy.scala](https://github.com/spotify/ratatool/blob/master/ratatool-diffy/src/main/scala/com/spotify/ratatool/diffy/BigDiffy.scala)


## Usage

```
BigDiffy - pair-wise field-level statistical diff
Usage: BigDiffy [dataflow_options] [options]

  --mode=[avro|bigquery]
  --key=<key>            '.' separated key field
  --lhs=<path>           LHS File path or BigQuery table
  --rhs=<path>           RHS File path or BigQuery table
  --output=<output>      File path prefix for output
  --ignore=<keys>        ',' separated field list to ignore
  --unordered=<keys>     ',' separated field list to treat as unordered
  [--with-header]        Output all TSVs with header rows
```

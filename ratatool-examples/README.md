Examples
=======

## Scalacheck
These examples cover different use cases for generating Avro or TableRow data with Ratatool and Scalacheck.
 The constraints are based on arbitrary criteria defined for [Avro](https://github.com/spotify/ratatool/blob/master/ratatool-examples/src/main/avro/schema.avsc)
 and [BigQuery](https://github.com/spotify/ratatool/blob/master/ratatool-examples/src/main/resources/schema.json)
 which should mirror some real life use cases of generating data where some fields have expected values
 or behaviour. It is recommended to do some reading on ScalaCheck and how Generators work before digging into
 these examples. Some resources are provided [here](https://github.com/spotify/ratatool/wiki/Generators).
 
## Diffy
Contains an example of using BigDiffy with Protobuf programmatically, as this is not currently supported
 in the CLI. This should serve as a reasonable workaround for users to build their own specific pipelines
 until a more generic version can be made.
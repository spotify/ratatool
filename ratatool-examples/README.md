Examples
=======

These example cover different use cases for generating Avro or TableRow data with Ratatool and Scalacheck.
 The constraints are based on arbitrary criteria defined for [Avro](https://github.com/spotify/ratatool/blob/master/ratatool-examples/src/main/avro/schema.avsc)
 and [BigQuery](https://github.com/spotify/ratatool/blob/master/ratatool-examples/src/main/resources/schema.json)
 which should mirror some real life use cases of generating data where some fields have expected values
 or behaviour. It is recommended to do some reading on ScalaCheck and how Generators work before digging into
 these examples. Some resources are provided [here](https://github.com/spotify/ratatool/wiki/Generators).
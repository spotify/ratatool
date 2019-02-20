Parquet Sampler
===============
Contains record sampling for Parquet.  Only operates in `head` mode where it samples record from the
start. We are pulling `ParquetSampler` into its own project in order to isolate the 
dependency on hadoop. This will no longer be accessible from the command line but can still be used
as shown below. Please open an issue if the change has caused problems for you.

## Usage
```scala
  val data = new ParquetSampler(stringPathToInputFile).sample(numberToSample, true)
  ParquetIO.writeToFile(data, data.head.getSchema, stringPathToOutputFile)
```


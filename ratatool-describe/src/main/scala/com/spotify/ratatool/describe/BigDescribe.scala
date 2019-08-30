package com.spotify.ratatool.describe

import com.spotify.ratatool.Command
import com.spotify.ratatool.io.AvroIO
import com.spotify.scio.avro._
import com.spotify.scio.{ContextAndArgs, ScioContext}
import io.circe.syntax._
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.options.PipelineOptions
import org.slf4j.LoggerFactory


object BigDescribe extends Command {
  val command: String = "bigDescribe"

  private val log = LoggerFactory.getLogger(BigDescribe.getClass)

  private def usage(): Unit = {
    // scalastyle:off regex line.size.limit
    println(
      s"""BigDescribe - descriptive statistics of big datasets
         |Usage: ratatool $command [dataflow_options] [options]
         |
         |  --input=<path>                             Input file path
         |  --output=<path>                            Output file path
         |  [--fields=<field1,field2,...>]             An optional list of fields to describe, eg if not all types are supported
         |
         |Since this runs a Scio/Beam pipeline, Dataflow options will have to be provided. At a
         |minimum, the following should be specified:
         |
         |   --project=<gcp-project-id>                GCP Project used to run your job
         |   --runner=DataflowRunner                   Executes the job on Google Cloud Dataflow
         |   --tempLocation=<gcs-path>                 Location for temporary files. GCS bucket must be created prior to running job.
         |
         |The following options are recommended, but may not be necessary.
         |
         |   --serviceAccount=<your-service-account>   Service account used on Dataflow workers. Useful to avoid permissions issues.
         |   --workerMachineType=<machine-type>        Can be tweaked based on your specific needs, but is not necessary.
         |   --maxNumWorkers=<num-workers>             Limits the number of workers (machines) used in the job to avoid using up quota.
         |
         |For more details regarding Dataflow options see here: https://cloud.google.com/dataflow/pipelines/specifying-exec-params
      """.stripMargin)
    // scalastyle:on regex line.size.limit
    sys.exit(1)
  }

  def main(cmdlineArgs: Array[String]): Unit = run(cmdlineArgs)

  def run(argv: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(argv)
    val (opts, _) = ScioContext.parseArguments[PipelineOptions](argv)
    FileSystems.setDefaultPipelineOptions(opts)

    val (inputArg, outputArg) = try {
      (args("input"), args("output"))
    } catch {
      case e: Throwable =>
        usage()
        throw e
    }

    val schema = AvroIO.getAvroSchemaFromFile(inputArg)
    val input = sc
      .withName("Read Avro Input")
      .avroFile[GenericRecord](inputArg, schema)

    BigDescribeAvro
      .pipeline(input)
      .withName("to JSON")
      .map(_.asJson.spaces2)
      .saveAsTextFile(outputArg, suffix = ".json")

    sc.close().waitUntilDone()
  }
}

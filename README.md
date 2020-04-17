# spark-dataproc-bq-st-api-demo
## !!! Caution: This example uses considerable resources when run on Google Cloud and can rake up a significant bill

This demonstrates using Spark on Google Cloud Dataproc using Bigquery Storage API to efficiently read a considerable amount of data and run a multi-stage Spark analytics process.

### Usage

#### Local Run

For local run, we need access to the Spark jars. So we need to edit build.sbt and comment out scope "provided" for the spark dependencies.
1. Comment out `% provided` part in build.sbt for `spark-core` and `spark-sql` packages

2. Run the following command from the project root directory: <br>
`sbt "runMain gcp.demo.SparkBQExample --local"`


#### Cluster Run
TODO

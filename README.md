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
Here we use an autoscaling Dataproc cluster with preemptible VMs. More details can be found at https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/autoscaling and https://cloud.google.com/dataproc/docs/concepts/compute/preemptible-vms.
<br>
We have included an example configuration to create an autoscaling policy. <br>
To create an autoscaling policy, run the command: <br>
`gcloud beta dataproc autoscaling-policies  import spark-demo-autoscal-policy --source example-dataproc-autoscaling-policy.yaml`

Once the policy is created, a new Dataproc autoscaling cluster can be created using a command like: <br>
`gcloud beta dataproc clusters create [CLUSTERNAME] \
 --region [REGION] \
 --zone [ZONE] \
 --master-machine-type n1-standard-4 \
 --num-workers 2 \
 --worker-machine-type n1-standard-4 \
 --scopes 'https://www.googleapis.com/auth/cloud-platform' \
 --image-version 1.5-debian10 \
 --enable-component-gateway \
 --autoscaling-policy=spark-demo-autoscal-policy \
 --project [PROJECT]`
 
 Feel free to play with different values for machinetypes, master and worker nodes.
<br>
Once the cluster is up and running, the following command can be used to run this program on the cluster: <br>
`gcloud dataproc jobs submit spark --cluster test-cluster --region us-central1 \
   --jars /Users/ranadip/IdeaProjects/sparkdemo/target/scala-2.12/sparkdemo-assembly-0.1.jar --class gcp.demo.SparkBQExample`
   

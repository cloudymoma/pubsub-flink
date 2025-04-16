### Flink job that consumes messages from Google Cloud Pubsub

**Known Issues**:

1. [Flink(Dataproc) PHS server](https://github.com/cloudymoma/pubsub-flink/blob/main/flink.sh#L64-L67) is **NOT** working properly by now. Hence you may skip the step for creating a PHS sever and check the flink/yarn job directly from the job server.

2. [Apache Flink Pubsub connector](https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/datastream/pubsub/) may **NOT** working properly in some cases. Hence this [GCP Flink Pubsub connector](https://github.com/GoogleCloudPlatform/pubsub) may worth a try.

https://github.com/cloudymoma/pubsub/tree/da-pom-fix I forked this GCP connector and made the `da-pom-fix` works so far.

this will build the flink connector GCP DA Fixed version and install on your local maven repository for later use.

```shell
cd pubsub/flink-connector
mvn clean package install -DskipTests
```

you can use `--useGcpPubsubConnectors <boolean>` in [`makefile`](https://github.com/cloudymoma/pubsub-flink/blob/main/makefile#L20) to switch between the two connectors.

#### before start

you need to change the settings in [`flink.sh`](https://github.com/cloudymoma/pubsub-flink/blob/main/flink.sh#L5-L26). These are the settings about your GCP project, region, bucket, cluster names etc.

Same for the [`makefile`](https://github.com/cloudymoma/pubsub-flink/blob/main/flink.sh#L5-L26)

It's very important to keep the `makefile` where you copy the compiled monolithic jar same path as the jar specified in `flink.sh`

Also, in the `makefile`, I have copied the Google Cloud Platform service account file from `$GOOGLE_APPLICATION_CREDENTIALS` this enviroment variable. You may need to change the path according to your own setup. Alternatively, you can grant your dataflow proper IAM permissions instead. More details [here](https://cloud.google.com/dataflow/docs/concepts/security-and-permissions#permissions)

1. Create a PHS server

```shell
./flink.sh flinkhistserver
```

2. Create a flink job server
```shell
./flink.sh flinkjobserver
```

3. Build the flink job and upload to GCS
```shell
make build
```

4. Run the flink job on the flink job server
```shell
make run
```

pwd := $(shell pwd)
ipaddr := $(shell hostname -I | cut -d ' ' -f 1)
gcp_project := du-hast-mich
pubsub_subscription := dingo-topic-sub-flink
gcs_bucket := dingoproc
gcs_flink_path := gs://$(gcs_bucket)/flink_jobs

build:
	@echo "Building Flink JAR..."
	@mvn clean package -DskipTests
	@echo "Copying Flink JAR to GCS..."
	@gsutil cp target/pubsub-flink-1.0-SNAPSHOT.jar $(gcs_flink_path)/pubsub-flink-1.0-SNAPSHOT.jar
	@gsutil cp $(GOOGLE_APPLICATION_CREDENTIALS) $(gcs_flink_path)/sa.json

run:
	@echo "Running Flink job on Dataproc... with service account: $(GOOGLE_APPLICATION_CREDENTIALS)"
	@./flink.sh flinkjob \
		--project $(gcp_project) \
		--subscription $(pubsub_subscription) \
		--useGcpPubsubConnectors true \
		--saCredentials $(gcs_flink_path)/sa.json
	@echo "Flink job submitted to Dataproc."

clean:
	@mvn clean

.PHONY: build run clean
package bindiego;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.gcp.pubsub.PubSubSource;
import org.apache.flink.util.Collector;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.auth.oauth2.GoogleCredentials; // For Service Account auth
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.time.Duration;
import java.io.FileInputStream;        // For reading key file
import java.io.FileNotFoundException;  // Specific exception for clarity
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels; 

public class BindiegoFlink {

    private static final Logger LOG = LoggerFactory.getLogger(BindiegoFlink.class);

    private static final String GCS_PREFIX = "gs://";

    public static void main(String[] args) throws Exception {
        // 1. Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. Parse command-line arguments
        final ParameterTool params = ParameterTool.fromArgs(args);

        // Required parameters
        final String projectId = params.getRequired("project");
        final String subscription = params.getRequired("subscription");
        final String saCredentials = params.getRequired("saCredentials");

        // Optional: checkpointing interval in ms (recommended for fault tolerance)
        final long checkpointInterval = params.getLong("checkpointInterval", 60000); // Default 60 seconds
        if (checkpointInterval > 0) {
             env.enableCheckpointing(checkpointInterval);
        }

        LOG.info("Starting Flink PubSub Window Count Job.");
        LOG.info("Reading from Project: {}, Subscription: {}", projectId, subscription);
        if (checkpointInterval > 0) {
             LOG.info("Checkpointing enabled every {} ms", checkpointInterval);
        } else {
             LOG.warn("Checkpointing is disabled. Job will restart from scratch on failure.");
        }

        // 3. Create Pub/Sub Source
        // Assumes Application Default Credentials (ADC) are configured
        // gcloud auth application-default login
        PubSubSource<String> pubSubSource = PubSubSource.newBuilder()
            .withDeserializationSchema(new SimplePubSubDeserializationSchema())
            .withProjectName(projectId)
            .withSubscriptionName(subscription)
            .withCredentials(ServiceAccountCredentials.fromStream(
                    BindiegoFlink.getInputStreamFromGcs(saCredentials) // Use the method to get InputStream
            ))
            .withPubSubSubscriberFactory(200, Duration.ofSeconds(15), 3) // batch size, timeout, max retries
            .build();

        // 4. Create DataStream from Pub/Sub Source
        DataStream<String> messageStream = env
            .addSource(pubSubSource);

        // 5. Apply Sliding Window and Process
        DataStream<Tuple2<Long, String>> windowedCounts = messageStream
            .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(1)))
            .process(new CountAndSampleWindowFunction());

        // 6. Print results to console (or sink to another system)
        windowedCounts.print(); // Prints to task manager logs

        // 7. Execute the Flink job
        env.execute("Flink PubSub Sliding Window Count");
    }

    /**
     * A ProcessAllWindowFunction that counts elements in a window and picks the first as a sample.
     */
    public static class CountAndSampleWindowFunction
            extends ProcessAllWindowFunction<String, Tuple2<Long, String>, TimeWindow> {

        @Override
        public void process(Context context, 
                Iterable<String> elements, 
                Collector<Tuple2<Long, String>> out) throws Exception {
            long count = 0;
            String sample = null;
            Iterator<String> iterator = elements.iterator();

            while (iterator.hasNext()) {
                String element = iterator.next();
                if (sample == null) { // Take the first element as sample
                    sample = element;
                }
                count++;
            }

            // Only output if the window wasn't empty
            if (count > 0) {
                // Format the sample to avoid excessively long log lines if needed
                String sampleOutput = (sample != null && sample.length() > 100) ? sample.substring(0, 97) + "..." : sample;
                if(sampleOutput == null) sampleOutput = "[No Sample Available]"; // Handle case where iterator was somehow empty after check

                LOG.debug("Window {}: Count={}, Sample='{}'", context.window(), count, sampleOutput);
                out.collect(new Tuple2<>(count, sampleOutput));
            } else {
                 LOG.debug("Window {}: Empty window.", context.window());
            }
        }
    }

    public static InputStream getInputStreamFromGcs(final String fullGcsPath)
            throws IOException, StorageException, IllegalArgumentException, FileNotFoundException {

        if (fullGcsPath == null || !fullGcsPath.startsWith(GCS_PREFIX)) {
            throw new IllegalArgumentException("Invalid GCS path format: Must start with '" 
                + GCS_PREFIX + "'. Path: " + fullGcsPath);
        }

        String pathWithoutPrefix = fullGcsPath.substring(GCS_PREFIX.length());
        int firstSlashIndex = pathWithoutPrefix.indexOf('/');

        if (firstSlashIndex <= 0) {
            throw new IllegalArgumentException("Invalid GCS path format: Missing bucket or object name after '" 
                + GCS_PREFIX + "'. Path: " + fullGcsPath);
        }

        final String bucketName = pathWithoutPrefix.substring(0, firstSlashIndex);
        final String objectName = pathWithoutPrefix.substring(firstSlashIndex + 1);

        Storage storage = StorageOptions.getDefaultInstance().getService();

        BlobId blobId = BlobId.of(bucketName, objectName);
        Blob blob = storage.get(blobId);

        if (blob == null) {
            throw new FileNotFoundException("GCS object not found: " + fullGcsPath);
        }

        ReadChannel reader = blob.reader();
        return Channels.newInputStream(reader);
    }
}
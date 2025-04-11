package bindiego;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubDeserializationSchema;
import com.google.pubsub.v1.PubsubMessage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * A simple PubSubDeserializationSchema that treats the message payload as a UTF-8 String.
 */
public class SimplePubSubDeserializationSchema implements PubSubDeserializationSchema<String> {

    @Override
    public String deserialize(PubsubMessage message) throws IOException {
        // Extract payload and convert to String
        if (message != null && message.getData() != null) {
            return message.getData().toStringUtf8();
        }
        return null; // Or throw an exception, or return a default value
    }

    @Override
    public boolean isEndOfStream(String nextElement) {
        // Pub/Sub is typically an endless stream
        return false;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        // Return TypeInformation for the produced type (String)
        return TypeInformation.of(String.class);
    }
}
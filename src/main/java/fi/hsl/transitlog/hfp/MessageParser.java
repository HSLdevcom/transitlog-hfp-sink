package fi.hsl.transitlog.hfp;

import com.dslplatform.json.DslJson;
import com.dslplatform.json.runtime.Settings;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.regex.Pattern;

public class MessageParser {
    private static final Logger log = LoggerFactory.getLogger(MessageParser.class);

    static final Pattern topicVersionRegex = Pattern.compile("(^v\\d+|dev)");

    // Let's use dsl-json (https://github.com/ngs-doo/dsl-json) for performance.
    // Based on this benchmark: https://github.com/fabienrenaud/java-json-benchmark

    //Example: https://github.com/ngs-doo/dsl-json/blob/master/examples/MavenJava8/src/main/java/com/dslplatform/maven/Example.java

    //Note! Apparently not thread safe, for per thread reuse use ThreadLocal pattern or create separate instances
    final DslJson<Object> dslJson = new DslJson<>(Settings.withRuntime().allowArrayFormat(true).includeServiceLoader());

    public static MessageParser newInstance() {
        return new MessageParser();
    }

    public HfpMessage parse(MqttMessage message) throws IOException {
        return parse(message.getPayload());
    }

    public HfpMessage parse(byte[] data) throws IOException {
        return dslJson.deserialize(HfpMessage.class, data, data.length);
    }

    public Optional<HfpMessage> safeParse(MqttMessage message) {
        try {
            return Optional.ofNullable(parse(message));
        }
        catch (Exception e) {
            log.error("Failed to parse message {}", new String(message.getPayload()));
            return Optional.empty();
        }
    }

    public static Optional<HfpMetadata> safeParseMetadata(String topic) throws Exception {
        try {
            return parseMetadata(topic);
        }
        catch (Exception e) {
            log.error("Failed to parse message metadata from topic " + topic, e);
            return Optional.empty();
        }
    }

    public static Optional<HfpMetadata> parseMetadata(String topic) throws Exception {
        return parseMetadata(topic, OffsetDateTime.now());
    }

    public static Optional<HfpMetadata> parseMetadata(String topic, OffsetDateTime receivedAt) throws Exception {
        log.debug("Parsing metadata from topic: " + topic);

        final String[] parts = topic.split("/");

        final HfpMetadata meta = new HfpMetadata();
        meta.received_at = receivedAt;
        //We first find the index of version. The prefix topic part can consist of more complicated path
        int versionIndex = findVersionIndex(parts);
        if (versionIndex < 0) {
            log.error("Failed to find topic version from topic " + topic);
            return Optional.empty();
        }


        meta.topic_prefix = joinFirstNParts(parts, versionIndex, "/");
        int index = versionIndex;
        meta.topic_version = parts[index++];
        // "/hfp/v1/journey/ongoing/bus/0022/00854/4555B/2/Leppävaara/19:56/4150264/5/60;24/28/65/06");
        meta.journey_type = HfpMetadata.JourneyType.valueOf(parts[index++]);
        meta.is_ongoing = parts[index++] == "ongoing";
        meta.mode = HfpMetadata.TransportMode.fromString(parts[index++]);
        meta.owner_operator_id = Integer.parseInt(parts[index++]);
        meta.vehicle_number = Integer.parseInt(parts[index++]);


    /*
    public JourneyType journey_type;
    public boolean is_ongoing;
    public TransportMode mode;
    public int owner_operator_id;
    public int vehicle_number;
    public String unique_vehicle_id;
    public Optional<String> route_id;
    public Optional<Integer> direction_id;
    public Optional<String> headsign;
    public Optional<LocalTime> journey_start_time;
    public Optional<String> next_stop_id;
    public Optional<Integer> geohash_level;
    public Optional<Double> topic_latitude;
    public Optional<Double> topic_longitude;
     */


        return Optional.of(meta);
    }

    static String joinFirstNParts(String[] parts, int upToIndexExcludingThis, String delimiter) {
        StringBuffer buffer = new StringBuffer();
        int index = 0;

        buffer.append(delimiter);
        while (index < upToIndexExcludingThis - 1) {
            index++;
            buffer.append(parts[index]);
            buffer.append(delimiter);
        }
        return buffer.toString();
    }

    public static int findVersionIndex(String[] parts) {
        for (int n = 0; n < parts.length; n++) {
            String p = parts[n];
            if (topicVersionRegex.matcher(p).matches()) {
                return n;
            }
        }
        return -1;
    }
}

package fi.hsl.transitlog.hfp;

import com.dslplatform.json.DslJson;
import com.dslplatform.json.runtime.Settings;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MessageParser {
    private static final Logger log = LoggerFactory.getLogger(MessageParser.class);

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

    public HfpMessage safeParse(MqttMessage message) {
        try {
            return parse(message);
        }
        catch (Exception e) {
            log.error("Failed to parse message {}", new String(message.getPayload()));
            return null;
        }
    }

}

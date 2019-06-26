package fi.hsl.transitlog.hfp;

import com.typesafe.config.Config;
import fi.hsl.common.hfp.HfpJson;
import fi.hsl.common.hfp.HfpParser;
import fi.hsl.common.hfp.proto.Hfp;

import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MessageProcessor implements IMqttMessageHandler {

    private static final Logger log = LoggerFactory.getLogger(MessageProcessor.class);

    final ArrayList<Hfp.Data> queue;
    final int QUEUE_MAX_SIZE = 100000;
    final QueueWriter writer;

    ScheduledExecutorService scheduler;

    final MqttConnector connector;

    private boolean shutdownInProgress = false;

    private final HfpParser parser = HfpParser.newInstance();

    private MessageProcessor(MqttConnector connector, QueueWriter w) {
        queue = new ArrayList<>(QUEUE_MAX_SIZE);
        writer = w;
        this.connector = connector;
    }

    public static MessageProcessor newInstance(Config config, MqttConnector connector, QueueWriter writer) throws Exception {
        final long intervalInMs = config.getDuration("application.dumpInterval", TimeUnit.MILLISECONDS);

        MessageProcessor processor = new MessageProcessor(connector, writer);
        log.info("Let's start the dump-executor");
        processor.startDumpExecutor(intervalInMs);
        return processor;
    }

    void startDumpExecutor(long intervalInMs) {
        log.info("Dump interval {} seconds", intervalInMs);
        scheduler = Executors.newSingleThreadScheduledExecutor();
        log.info("Starting result-scheduler");

        scheduler.scheduleAtFixedRate(() -> {
            try {
                dump();
            }
            catch (Exception e) {
                log.error("Failed to check results, closing application", e);
                close(true);
            }
        }, intervalInMs, intervalInMs, TimeUnit.MILLISECONDS);
    }

    private void dump() throws Exception {
        log.debug("Saving results");
        ArrayList<Hfp.Data> copy;
        synchronized (queue) {
            copy = new ArrayList<>(queue);
            queue.clear();
        }

        if (copy.isEmpty()) {
            log.warn("Queue empty, no messages to write to database");
        }
        else {
            log.info("Writing {} messages to database", copy.size());
            writer.write(copy);
        }
    }

    @Override
    public void handleMessage(String topic, MqttMessage message) throws Exception {
        if (queue.size() > QUEUE_MAX_SIZE) {
            //TODO think what to do if queue is full!
            log.error("Queue full: " + QUEUE_MAX_SIZE);
            return;
        }

        try {
            // This method is invoked synchronously by the MQTT client (via our connector), so all events arrive in the same thread
            // https://www.eclipse.org/paho/files/javadoc/org/eclipse/paho/client/mqttv3/MqttCallback.html

            // Optimally we would like to send the event to Pulsar synchronously and validate that it was a success,
            // and only after that acknowledge the message to mqtt by returning gracefully from this function.
            // This works if the rate of incoming messages is low enough to complete the Pulsar transaction.
            // This would allow us to deliver messages once and only once, in insertion order

            // If we want to improve our performance and lose guarantees of once-and-only-once,
            // we can optimize the pipeline by sending messages asynchronously to Pulsar.
            // This means that data will be lost on insertion errors.
            // Using a single producer however should guarantee insertion-order guarantee between two consecutive messages.

            // Current implementation uses the latter approach

            if (!connector.isConnected()) {
                log.error("MQTT client is no longer connected. Exiting application");
                throw new Exception("MQTT client is no longer connected");
            }

            long now = System.currentTimeMillis();

            byte[] payload = message.getPayload();

            if (payload != null) {
                Hfp.Data data = parseData(topic, payload, now);
                synchronized (queue) {
                    queue.add(data);
                }
            }
            else {
                log.warn("Cannot process MQTT message because content is null");
            }

        }
        catch (Exception e) {
            log.error("Error while handling the message", e);
            // Let's close everything and restart.
            // Closing the MQTT connection should enable us to receive the same message again.
            close(true);
            throw e;
        }
    }

    Hfp.Data parseData(String rawTopic, byte[] rawPayload, long timestamp) throws Exception {
        final HfpJson jsonPayload = parser.parseJson(rawPayload);
        Hfp.Payload payload = HfpParser.parsePayload(jsonPayload);
        Hfp.Topic topic = HfpParser.parseTopic(rawTopic, timestamp);

        Hfp.Data.Builder builder = Hfp.Data.newBuilder();
        builder.setSchemaVersion(builder.getSchemaVersion())
                .setPayload(payload)
                .setTopic(topic);
        return builder.build();
    }

    @Override
    public void connectionLost(Throwable cause) {
        log.info("Mqtt connection lost");
        close(false);
    }

    public void close(boolean closeMqtt) {
        if (shutdownInProgress) {
            return;
        }
        shutdownInProgress = true;

        log.warn("Closing MessageProcessor resources");
        scheduler.shutdown();
        log.info("Scheduler shutdown finished");

        log.warn("Closing MessageProcessor resources");
        //Let's first close the MQTT to stop the event stream.
        if (closeMqtt) {
            connector.close();
            log.info("MQTT connection closed");
        }
    }
}

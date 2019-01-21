package fi.hsl.transitlog.hfp;

import com.typesafe.config.Config;
import fi.hsl.transitlog.mqtt.IMqttMessageHandler;
import fi.hsl.transitlog.mqtt.MqttApplication;

import org.eclipse.paho.client.mqttv3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MessageProcessor implements IMqttMessageHandler {

    private static final Logger log = LoggerFactory.getLogger(MessageProcessor.class);

    final ArrayList<HfpData> queue;
    final int QUEUE_MAX_SIZE = 100000;
    final MessageParser parser = MessageParser.newInstance();
    final QueueWriter writer;
    final MqttApplication app;

    ScheduledExecutorService scheduler;

    private MessageProcessor(MqttApplication app, QueueWriter writer) {
        queue = new ArrayList<>(QUEUE_MAX_SIZE);
        this.app = app;
        this.writer = writer;
    }

    public static MessageProcessor newInstance(Config config, MqttApplication app, QueueWriter writer) throws Exception {
        final long intervalInMs = config.getDuration("application.dumpInterval", TimeUnit.MILLISECONDS);

        MessageProcessor processor = new MessageProcessor(app, writer);
        log.info("MessageProcessor subscribing to MQTT Application");
        app.subscribe(processor);

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
                close();
            }
        }, intervalInMs, intervalInMs, TimeUnit.MILLISECONDS);
    }

    private void dump() {
        log.debug("Saving results");
        //TODO implement
    }

    @Override
    public void handleMessage(String topic, MqttMessage message) throws Exception {
        if (queue.size() > QUEUE_MAX_SIZE) {
            log.warn("Queue full: " + QUEUE_MAX_SIZE);
            return;
        }

        Optional<HfpMetadata> maybeMetadata = MessageParser.safeParseMetadata(topic);
        if (!maybeMetadata.isPresent()) {
            log.warn("Failed to parse hfp metadata from MQTT topic");
        }


        Optional<HfpMessage> maybeHfp = parser.safeParse(message);
        if (!maybeHfp.isPresent()) {
            log.warn("Failed to parse hfp payload from MQTT message");
        }

        if (maybeHfp.isPresent() && maybeMetadata.isPresent()) {
            queue.add(new HfpData(maybeMetadata.get(), maybeHfp.get()));
        }

        if (queue.size() % 1000 == 0) {
            log.debug("Got messages: " + queue.size());
            writer.write(queue);
            queue.clear();
        }

        /*
        if (queue.size() > QUEUE_MAX_SIZE) {
            log.warn("Queue full, removing oldest message");
            queue.removeFirst();
        }*/
    }

    public void close() {
        log.warn("Closing application");
        scheduler.shutdown();
        log.info("Scheduler shutdown finished");
        app.close();
        log.info("Application closed");
    }
}

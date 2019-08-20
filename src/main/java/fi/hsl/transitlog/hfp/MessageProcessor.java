package fi.hsl.transitlog.hfp;

import fi.hsl.common.hfp.proto.Hfp;
import fi.hsl.common.pulsar.IMessageHandler;

import fi.hsl.common.pulsar.PulsarApplication;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.TransitdataSchema;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MessageProcessor implements IMessageHandler {

    private static final Logger log = LoggerFactory.getLogger(MessageProcessor.class);

    final ArrayList<Hfp.Data> queue;
    final int QUEUE_MAX_SIZE = 100000;
    final QueueWriter writer;
    private final Consumer<byte[]> consumer;
    private final PulsarApplication application;

    ScheduledExecutorService scheduler;

    private MessageProcessor(PulsarApplication app, QueueWriter w) {
        queue = new ArrayList<>(QUEUE_MAX_SIZE);
        writer = w;
        consumer = app.getContext().getConsumer();
        application = app;
    }

    public static MessageProcessor newInstance(PulsarApplication app, QueueWriter writer) throws Exception {
        final long intervalInMs = app.getContext().getConfig().getDuration("application.dumpInterval", TimeUnit.MILLISECONDS);

        MessageProcessor processor = new MessageProcessor(app, writer);
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
            log.info("Queue empty, no messages to write to database");
        }
        else {
            log.info("Writing {} messages to database", copy.size());
            //writer.write(copy);
            writer.writeToMongoDB(copy);
        }
    }

    @Override
    public void handleMessage(Message message) throws Exception {
        if (queue.size() > QUEUE_MAX_SIZE) {
            //TODO think what to do if queue is full!
            log.error("Queue full: " + QUEUE_MAX_SIZE);
            return;
        }

        if (TransitdataSchema.hasProtobufSchema(message, TransitdataProperties.ProtobufSchema.HfpData)) {
            Hfp.Data data = Hfp.Data.parseFrom(message.getData());

            //Ignore event types other than VP until transitlog has support for other event types
            if (data.getTopic().getEventType() == Hfp.Topic.EventType.VP) {
                synchronized (queue) {
                    queue.add(data);
                }
            }
        }
        else {
            log.warn("Invalid protobuf schema, expecting HfpData");
        }
        ack(message.getMessageId());
    }

    private void ack(MessageId received) {
        consumer.acknowledgeAsync(received)
                .exceptionally(throwable -> {
                    log.error("Failed to ack Pulsar message", throwable);
                    return null;
                })
                .thenRun(() -> {});
    }

    public void close(boolean closePulsar) {
        log.warn("Closing MessageProcessor resources");
        scheduler.shutdown();
        log.info("Scheduler shutdown finished");
        if (closePulsar && application != null) {
            log.info("Closing also Pulsar application");
            application.close();
        }
    }

}

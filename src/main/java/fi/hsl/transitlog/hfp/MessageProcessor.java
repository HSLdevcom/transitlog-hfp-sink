package fi.hsl.transitlog.hfp;

import fi.hsl.common.hfp.proto.Hfp;
import fi.hsl.common.pulsar.IMessageHandler;

import fi.hsl.common.pulsar.PulsarApplication;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.TransitdataSchema;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MessageProcessor implements IMessageHandler {

    private static final Logger log = LoggerFactory.getLogger(MessageProcessor.class);

    final ArrayList<Hfp.Data> queue;
    final ArrayList<MessageId> msgQueue;
    final int QUEUE_MAX_SIZE = 250000;
    private boolean queueFull = false;
    private long queueClearTime;
    final QueueWriter writer;
    private final Consumer<byte[]> consumer;
    private final PulsarApplication application;
    private DecimalFormat df = new DecimalFormat("###.##");

    ScheduledExecutorService scheduler;

    private MessageProcessor(PulsarApplication app, QueueWriter w) {
        queue = new ArrayList<>(QUEUE_MAX_SIZE);
        msgQueue = new ArrayList<>(QUEUE_MAX_SIZE);
        queueClearTime = System.currentTimeMillis();
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
        log.info("Dump interval {} seconds", intervalInMs/1000);
        scheduler = Executors.newSingleThreadScheduledExecutor();
        log.info("Starting result-scheduler");

        scheduler.scheduleAtFixedRate(() -> {
            try {
                dump();
            } catch (Exception e) {
                log.error("Failed to check results, closing application", e);
                close(true);
            }
        }, intervalInMs, intervalInMs, TimeUnit.MILLISECONDS);
    }

    private void dump() throws Exception {
        log.debug("Saving results");
        ArrayList<Hfp.Data> copy;
        ArrayList<MessageId> msgQueueCopy;
        synchronized (queue) {
            copy = new ArrayList<>(queue);
            msgQueueCopy = new ArrayList<>(msgQueue);
            queue.clear();
            msgQueue.clear();
        }
        long toWriteCount = copy.size();
        if (toWriteCount > 0) {
            long writeStartTime = System.currentTimeMillis();
            double queueTtlTime = writeStartTime - queueClearTime;
            queueClearTime = System.currentTimeMillis();
            double msgRateIn = (queueTtlTime > 0) ? toWriteCount / (queueTtlTime / 1000.0) : 999999.9;
            log.info("Writing {} rows to database, msgRateIn was: {} msg/s, start time: {}", toWriteCount, df.format(msgRateIn), writeStartTime);
            boolean writeSuccess = writer.write(copy, writeStartTime);
            if (writeSuccess == true) {
                ackDeliveredMessages(msgQueueCopy);
            } else {
                log.error("Error in writing {} rows to db, start time: {}", copy.size(), writeStartTime);
            }
        } else {
            log.info("Queue empty, no messages to write to database");
        }
    }

    @Override
    public void handleMessage(Message message) throws Exception {
        if (queue.size() >= QUEUE_MAX_SIZE) {
            //TODO think what to do if queue is full (other than manually reset Pulsar cursor to read from message backlog)!
            if (Boolean.FALSE.equals(queueFull)) {
                log.error("Queue got full. Storing messages to Pulsar backlog from now on, manually re-subscribe these later with Pulsar-admin");
                queueFull = true;
            }
            return;
        } else {
            if (Boolean.TRUE.equals(queueFull)) { log.info("Queue not full anymore, size: " + queue.size()); }
            queueFull = false;
        }

        if (TransitdataSchema.hasProtobufSchema(message, TransitdataProperties.ProtobufSchema.HfpData)) {
            Hfp.Data data = Hfp.Data.parseFrom(message.getData());

            synchronized (queue) {
                queue.add(data);
                msgQueue.add(message.getMessageId());
            }
        } else {
            log.warn("Invalid protobuf schema, expecting HfpData");
        }
        // Messages should not be acked already here but only after successful write
    }

    private void ackDeliveredMessages(List<MessageId> messageIds) {
        for (MessageId msgId: messageIds) {
            ack(msgId);
        }
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

package fi.hsl.transitlog.hfp;

import com.typesafe.config.Config;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.pulsar.PulsarApplication;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.transitlog.mqtt.MqttConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Scanner;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);


    public static void main(String[] args) {
        log.info("Launching Transitdata-HFP-Source.");

        Config config = ConfigParser.createConfig();

        log.info("Configuration read, launching the main loop");
        MqttConnector connector = null;
        MessageProcessor processor = null;
        try (PulsarApplication app = PulsarApplication.newInstance(config)) {
            QueueWriter writer = QueueWriter.newInstance(config);
            processor = MessageProcessor.newInstance(config, writer);
            log.info("Starting to process messages");

            app.launchWithHandler();
        }
        catch (Exception e) {
            log.error("Exception at main", e);
            if (processor != null) {
                processor.close(false);
            }
            if (connector != null) {
                connector.close();
            }
        }

    }
}

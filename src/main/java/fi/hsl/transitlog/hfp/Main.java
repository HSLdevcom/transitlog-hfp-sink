package fi.hsl.transitlog.hfp;

import com.typesafe.config.Config;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.pulsar.PulsarApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);


    public static void main(String[] args) {
        log.info("Launching Transitdata-HFP-Source.");

        Config config = ConfigParser.createConfig();

        log.info("Configuration read, launching the main loop");
        MessageProcessor processor = null;
        QueueWriter writer = null;
        try (PulsarApplication app = PulsarApplication.newInstance(config)) {
            writer = QueueWriter.newInstance(config);
            processor = MessageProcessor.newInstance(app, writer);
            log.info("Starting to process messages");

            app.launchWithHandler(processor);
        }
        catch (Exception e) {
            log.error("Exception at main", e);
            if (processor != null) {
                processor.close(false);
            }
            if (writer != null) {
                writer.close();
            }
        }

    }
}

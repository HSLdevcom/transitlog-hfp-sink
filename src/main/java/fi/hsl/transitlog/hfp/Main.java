package fi.hsl.transitlog.hfp;

import com.typesafe.config.Config;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.config.ConfigUtils;
import fi.hsl.common.pulsar.PulsarApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Scanner;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);


    public static void main(String[] args) {
        log.info("Launching Transitdata-HFP-Sink.");

        Config config = ConfigParser.createConfig();

        log.info("Configuration read, launching the main loop");
        MessageProcessor processor = null;
        QueueWriter writer = null;
        try (PulsarApplication app = PulsarApplication.newInstance(config)) {
            final String connectionString = readConnectionString();
            writer = QueueWriter.newInstance(config, connectionString);
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

    private static String readConnectionString() throws Exception {
        String connectionString = "";
        try {
            //Default path is what works with Docker out-of-the-box. Override with a local file if needed
            final String secretFilePath = ConfigUtils.getEnv("FILEPATH_CONNECTION_STRING")
                    .orElse("/run/secrets/db_conn_string");
            connectionString = new Scanner(new File(secretFilePath))
                    .useDelimiter("\\Z").next();
        } catch (Exception e) {
            log.error("Failed to read DB connection string from secrets", e);
            throw e;
        }

        if (connectionString.isEmpty()) {
            throw new Exception("Failed to find DB connection string, exiting application");
        }
        return connectionString;
    }
}

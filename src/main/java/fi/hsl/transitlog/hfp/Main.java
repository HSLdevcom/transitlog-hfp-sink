package fi.hsl.transitlog.hfp;

import com.typesafe.config.Config;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.config.ConfigUtils;
import fi.hsl.common.health.HealthServer;
import fi.hsl.common.pulsar.PulsarApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.BooleanSupplier;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        log.info("Launching Transitdata-HFP-Sink.");

        Config config = ConfigParser.createConfig();

        MessageProcessor processor = null;
        QueueWriter writer = null;
        MqttConnector connector = null;
        try {
            final String connectionString = ConfigUtils.getConnectionStringFromFileOrThrow(Optional.of("/run/secrets/db_conn_string"));
            writer = QueueWriter.newInstance(config, connectionString);

            Optional<Credentials> credentials = Credentials.readMqttCredentials(config);

            log.info("Configurations read, connecting.");

            connector = new MqttConnector(config, credentials);
            final MqttConnector connectorRef = connector;

            // Add custom health check for MQTT connection
            final BooleanSupplier connectorHealthCheck = () -> {
                if (connectorRef != null) {
                    return connectorRef.isConnected();
                }
                return false;
            };
            PulsarApplication app = PulsarApplication.newInstance(config);
            HealthServer healthServer = app.getContext().getHealthServer();
            if (healthServer != null) {
                healthServer.addCheck(connectorHealthCheck);
            }

            processor = MessageProcessor.newInstance(config, connector, writer);
            //Let's subscribe to connector before connecting so we'll get all the events.
            connector.subscribe(processor);

            connector.connect();

            log.info("Connections established, let's process some messages");
        }
        catch (Exception e) {
            log.error("Exception at main", e);
            if (processor != null) {
                processor.close(false);
            }
            if (writer != null) {
                writer.close();
            }
            if (connector != null) {
                connector.close();
            }
        }
    }
}

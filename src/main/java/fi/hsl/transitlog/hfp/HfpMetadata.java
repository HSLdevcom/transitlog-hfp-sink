package fi.hsl.transitlog.hfp;

import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Optional;

public class HfpMetadata {
    public enum JourneyType {
        journey, deadrun
    }

    public enum TransportMode {
        bus, train, tram, metro, ferry
    }

    public OffsetDateTime received_at;
    public String topic_prefix;
    public String topic_version;
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

}

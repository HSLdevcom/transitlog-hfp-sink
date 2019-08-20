package fi.hsl.transitlog.hfp.models;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;

public class HFP {
    public ZonedDateTime received_at;
    public String topic_prefix;
    public String topic_version;
    public String journey_type;
    public boolean is_ongoing;
    public String event_type;
    public String mode;
    public int owner_operator_id;
    public int vehicle_number;
    public String unique_vehicle_id;
    public String route_id;
    public int direction_id;
    public String headsign;
    public LocalTime journey_start_time;
    public String next_stop_id;
    public int geohash_level;
    public double topic_latitude;
    public double topic_longitude;
    public String desi;
    public Integer dir;
    public int oper;
    public int veh;
    public ZonedDateTime tst;
    public long tsi;
    public double spd;
    public int hdg;
    public double lat;

    private double _long;

    public double getLong() {
        return _long;
    }

    public void setLong(double longitude) {
        _long = longitude;
    }

    public double acc;
    public int dl;
    public double odo;
    public Boolean drst;
    public LocalDate oday;
    public int jrn;
    public int line;
    public LocalTime start;
    public String loc;
    public int stop;
    public String route;
    public int occu;
}

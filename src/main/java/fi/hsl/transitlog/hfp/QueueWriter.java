package fi.hsl.transitlog.hfp;

import com.mongodb.ConnectionString;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import com.mongodb.reactivestreams.client.*;
import com.typesafe.config.Config;
import fi.hsl.common.hfp.HfpParser;
import fi.hsl.common.hfp.proto.Hfp;
import fi.hsl.transitlog.hfp.models.HFP;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.*;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class QueueWriter {
    private static final Logger log = LoggerFactory.getLogger(QueueWriter.class);

    Connection connection;

    MongoClient mongoClient = MongoClients.create(new ConnectionString("mongodb://transitlog-hfp:aYNr04xGXLbuky8B1gmhTiZUcp23HhOlrGrLoVTaxQjALAZbTDxiH6xzC0c0XOciqmlYATCJek1LYPvMxtpayw==@transitlog-hfp.documents.azure.com:10255/?ssl=true&replicaSet=globaldb"));
    MongoDatabase mongoDatabase = mongoClient.getDatabase("hsl");

    private QueueWriter() {
    }

    private QueueWriter(Connection conn) {
        connection = conn;
    }

    public static QueueWriter newInstance(Config config, final String connectionString) throws Exception {
        log.info("Connecting to the database");
        //Connection conn = DriverManager.getConnection(connectionString);
        //conn.setAutoCommit(false); // we're doing batch inserts so no auto commit
        log.info("Connection success");
        //return new QueueWriter(conn);
        return new QueueWriter();
    }

    public void writeToMongoDB(List<Hfp.Data> messages) {
        MongoCollection<HFP> vehicles = mongoDatabase.getCollection("vehicles", HFP.class);
        List<HFP> collect = messages.stream()
                .map(this::createHfp)
                .collect(Collectors.toList());
        vehicles.insertMany(collect).subscribe(new Subscriber<Success>() {
            @Override
            public void onSubscribe(Subscription subscription) {
            }

            @Override
            public void onNext(Success success) {

            }

            @Override
            public void onError(Throwable throwable) {
                log.error("Error while inserting rows to mongo", throwable);
            }

            @Override
            public void onComplete() {
                log.info("Inserted {} rows to mongo", messages.size());
            }
        });
    }

    public HFP createHfp(Hfp.Data data) {
        HFP hfp = new HFP();
        final Hfp.Topic meta = data.getTopic();
        hfp.received_at = ZonedDateTime.ofInstant(Instant.ofEpochSecond(meta.getReceivedAt()), ZoneOffset.UTC);
        hfp.topic_prefix = meta.getTopicPrefix();
        hfp.topic_version = meta.getTopicVersion();
        hfp.journey_type = meta.getJourneyType().toString();
        hfp.is_ongoing = meta.getTemporalType() == Hfp.Topic.TemporalType.ongoing;
        hfp.event_type = wrapToOptional(meta::hasEventType, meta::getEventType).map(eventType -> eventType.toString()).orElse(null);
        hfp.mode = wrapToOptional(meta::hasTransportMode, meta::getTransportMode).map(mode -> mode.toString()).orElse(null);
        hfp.owner_operator_id = meta.getOperatorId();
        hfp.vehicle_number = meta.getVehicleNumber();
        hfp.unique_vehicle_id = meta.getUniqueVehicleId();
        hfp.route_id = meta.getRouteId();
        hfp.direction_id = meta.getDirectionId();
        hfp.headsign = meta.getHeadsign();
        hfp.journey_start_time = wrapToOptional(meta::hasStartTime, meta::getStartTime).map(startTime -> LocalTime.parse(startTime)).orElse(null);
        hfp.next_stop_id = meta.getNextStop();
        hfp.geohash_level = meta.getGeohashLevel();
        hfp.topic_latitude = meta.getLatitude();
        hfp.topic_longitude = meta.getLongitude();
        //From payload:
        final Hfp.Payload message = data.getPayload();
        hfp.desi = message.getDesi();
        hfp.dir = wrapToOptional(message::hasDir, message::getDir).flatMap(HfpParser::safeParseInt).orElse(null);
        hfp.oper = message.getOper();
        hfp.veh = message.getVeh();
        hfp.tst = ZonedDateTime.parse(message.getTst());
        hfp.tsi = message.getTsi();
        hfp.spd = message.getSpd();
        hfp.hdg = message.getHdg();
        hfp.lat = message.getLat();
        hfp.setLong(message.getLong());
        hfp.acc = message.getAcc();
        hfp.dl = message.getDl();
        hfp.odo = message.getOdo();
        hfp.drst = wrapToOptional(message::hasDrst, message::getDrst).flatMap(HfpParser::safeParseBoolean).orElse(null);
        hfp.oday = wrapToOptional(message::hasOday, message::getOday).map(oday -> LocalDate.parse(oday)).orElse(null);
        hfp.jrn = message.getJrn();
        hfp.line = message.getLine();
        hfp.start = wrapToOptional(message::hasStart, message::getStart).map(start -> LocalTime.parse(start)).orElse(null);
        hfp.loc = wrapToOptional(message::hasLoc, message::getLoc).map(loc -> loc.toString()).orElse(null);
        hfp.stop = message.getStop();
        hfp.route = message.getRoute();
        hfp.occu = message.getOccu();
        return hfp;
    }

    static <T> Optional<T> wrapToOptional(Supplier<Boolean> isPresent, Supplier<T> getter) {
        if (isPresent.get()) {
            return Optional.of(getter.get());
        }
        return Optional.empty();
    }

    private WriteModel<HFP> mapToWriteModel(Hfp.Data data) {
        HFP hfp = createHfp(data);
        return new InsertOneModel<>(hfp);
    }

    public void write(List<Hfp.Data> messages) throws Exception {
        log.info("Writing {} rows to database", messages.size());

        long startTime = System.currentTimeMillis();
        String queryString = createInsertStatement();
        try (PreparedStatement statement = connection.prepareStatement(queryString)) {

            for (Hfp.Data data : messages) {
                int index = 1;

                final Hfp.Topic meta = data.getTopic();

                statement.setTimestamp(index++, Timestamp.from(Instant.ofEpochMilli(meta.getReceivedAt())));
                statement.setString(index++, meta.getTopicPrefix());
                statement.setString(index++, meta.getTopicVersion());
                statement.setString(index++, meta.getJourneyType().toString());
                statement.setBoolean(index++, meta.getTemporalType() == Hfp.Topic.TemporalType.ongoing);

                // Protobuf doesn't allow us to get an object which hasn't been set, so we always have to check of existance before.
                // JDBC Driver doesn't support Optionals nor does it stand leaving null values unset, so we need to explicitly insert nulls also.
                // => these cause some boilerplate here.

                Optional<String> maybeEventType = wrapToOptional(meta::hasEventType, meta::getEventType).map(eventType -> eventType.toString());
                setNullable(index++, maybeEventType.orElse(null), Types.VARCHAR, statement);

                Optional<String> maybeMode = wrapToOptional(meta::hasTransportMode, meta::getTransportMode).map(mode -> mode.toString());
                setNullable(index++, maybeMode.orElse(null), Types.VARCHAR, statement);

                statement.setInt(index++, meta.getOperatorId());
                statement.setInt(index++, meta.getVehicleNumber());
                statement.setString(index++, meta.getUniqueVehicleId());

                setNullable(index++, meta::hasRouteId, meta::getRouteId, Types.VARCHAR, statement);
                setNullable(index++, meta::hasDirectionId, meta::getDirectionId, Types.INTEGER, statement);
                setNullable(index++, meta::hasHeadsign, meta::getHeadsign, Types.VARCHAR, statement);

                Optional<Time> maybeStartTime = wrapToOptional(meta::hasStartTime, meta::getStartTime).flatMap(HfpParser::safeParseTime);
                setNullable(index++, maybeStartTime.orElse(null), Types.TIME, statement);
                setNullable(index++, meta::hasNextStop, meta::getNextStop, Types.VARCHAR, statement);
                setNullable(index++, meta::hasGeohashLevel, meta::getGeohashLevel, Types.INTEGER, statement);
                setNullable(index++, meta::hasLatitude, meta::getLatitude, Types.DOUBLE, statement);
                setNullable(index++, meta::hasLongitude, meta::getLongitude, Types.DOUBLE, statement);

                //From payload:
                final Hfp.Payload message = data.getPayload();
                setNullable(index++, message::hasDesi, message::getDesi, Types.VARCHAR, statement);

                Optional<Integer> maybeDirection = wrapToOptional(message::hasDir, message::getDir).flatMap(HfpParser::safeParseInt);
                setNullable(index++, maybeDirection.orElse(null), Types.INTEGER, statement);
                setNullable(index++, message::hasOper, message::getOper, Types.INTEGER, statement);

                statement.setInt(index++, message.getVeh());
                statement.setTimestamp(index++, HfpParser.safeParseTimestamp(message.getTst()).get()); // This field cannot be null
                statement.setLong(index++, message.getTsi());

                setNullable(index++, message::hasSpd, message::getSpd, Types.DOUBLE, statement);
                setNullable(index++, message::hasHdg, message::getHdg, Types.INTEGER, statement);
                setNullable(index++, message::hasLat, message::getLat, Types.DOUBLE, statement);
                setNullable(index++, message::hasLong, message::getLong, Types.DOUBLE, statement);
                setNullable(index++, message::hasAcc, message::getAcc, Types.DOUBLE, statement);
                setNullable(index++, message::hasDl, message::getDl, Types.INTEGER, statement);
                setNullable(index++, message::hasOdo, message::getOdo, Types.DOUBLE, statement);

                Optional<Boolean> maybeDoors = wrapToOptional(message::hasDrst, message::getDrst).flatMap(HfpParser::safeParseBoolean);
                setNullable(index++, maybeDoors.orElse(null), Types.BOOLEAN, statement);

                Optional<Date> maybeOperatingDay = wrapToOptional(message::hasOday, message::getOday).flatMap(HfpParser::safeParseDate);
                setNullable(index++, maybeOperatingDay.orElse(null), Types.DATE, statement);
                setNullable(index++, message::hasJrn, message::getJrn, Types.INTEGER, statement);
                setNullable(index++, message::hasLine, message::getLine, Types.INTEGER, statement);

                Optional<Time> maybeStartTimePayload = wrapToOptional(message::hasStart, message::getStart).flatMap(HfpParser::safeParseTime);
                setNullable(index++, maybeStartTimePayload.orElse(null), Types.TIME, statement);

                Optional<String> maybeLoc = wrapToOptional(message::hasLoc, message::getLoc).map(loc -> loc.toString());
                setNullable(index++, maybeLoc.orElse(null), Types.VARCHAR, statement);
                setNullable(index++, message::hasStop, message::getStop, Types.INTEGER, statement);
                setNullable(index++, message::hasRoute, message::getRoute, Types.VARCHAR, statement);
                setNullable(index++, message::hasOccu, message::getOccu, Types.INTEGER, statement);

                statement.addBatch();
            }

            statement.executeBatch();
            connection.commit();
        } catch (Exception e) {
            log.error("Failed to insert batch to database: ", e);
            connection.rollback();
            throw e;
        } finally {
            long elapsed = System.currentTimeMillis() - startTime;
            log.info("Total insert time: {} ms", elapsed);
        }
    }

    private String createInsertStatement() {
        return new StringBuffer()
                .append("INSERT INTO VEHICLES (")
                .append("received_at, topic_prefix, topic_version, ")
                .append("journey_type, is_ongoing, event_type, mode, ")
                .append("owner_operator_id, vehicle_number, unique_vehicle_id, ")
                .append("route_id, direction_id, headsign, ")
                .append("journey_start_time, next_stop_id, geohash_level, ")
                .append("topic_latitude, topic_longitude, ")
                .append("desi, dir, oper, ")
                .append("veh, tst, tsi, ")
                .append("spd, hdg, lat, ")
                .append("long, acc, dl, ")
                .append("odo, drst, oday, ")
                .append("jrn, line, start, ")
                .append("loc, stop, route, occu")
                .append(") VALUES (")
                .append("?, ?, ?, ?::JOURNEY_TYPE, ?, ?::EVENT_TYPE, ?::TRANSPORT_MODE, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ")
                .append("?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::LOCATION_QUALITY_METHOD, ?, ?, ?")
                .append(");")
                .toString();
    }

    private <T> void setNullable(int index, Supplier<Boolean> isPresent, Supplier<T> getter, int jdbcType, PreparedStatement statement) throws SQLException {
        Optional<T> maybeValue = wrapToOptional(isPresent, getter);
        T valueOrNull = maybeValue.orElse(null);
        setNullable(index, valueOrNull, jdbcType, statement);
    }

    private void setNullable(int index, Object value, int jdbcType, PreparedStatement statement) throws SQLException {
        if (value == null) {
            statement.setNull(index, jdbcType);
        } else {
            //This is just awful but Postgres driver does not support setObject(value, type);
            //Leaving null values not set is also not an option.
            switch (jdbcType) {
                case Types.BOOLEAN:
                    statement.setBoolean(index, (Boolean) value);
                    break;
                case Types.INTEGER:
                    statement.setInt(index, (Integer) value);
                    break;
                case Types.BIGINT:
                    statement.setLong(index, (Long) value);
                    break;
                case Types.DOUBLE:
                    statement.setDouble(index, (Double) value);
                    break;
                case Types.DATE:
                    statement.setDate(index, (Date) value);
                    break;
                case Types.TIME:
                    statement.setTime(index, (Time) value);
                    break;
                case Types.VARCHAR:
                    statement.setString(index, (String) value); //Not sure if this is correct, field in schema is TEXT
                    break;
                default:
                    log.error("Invalid jdbc type, bug in the app! {}", jdbcType);
                    break;
            }
        }
    }

    public void close() {
        log.info("Closing DB Connection");
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            log.error("Failed to close DB Connection", e);
        }

    }
}

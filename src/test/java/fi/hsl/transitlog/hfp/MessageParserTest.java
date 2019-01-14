package fi.hsl.transitlog.hfp;

import org.junit.Test;

import java.net.URL;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.Scanner;

import static org.junit.Assert.*;

public class MessageParserTest {
    @Test
    public void parseSampleFile() throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        URL url = classLoader.getResource("hfp-sample.json");

        String content = new Scanner(url.openStream(), "UTF-8").useDelimiter("\\A").next();

        HfpMessage hfp = MessageParser.newInstance().parse(content.getBytes("UTF-8"));
        assertNotNull(hfp);
        assertEquals("81", hfp.VP.desi);
        assertEquals("2", hfp.VP.dir);
        assertTrue(22 == hfp.VP.oper);
        assertTrue(792 == hfp.VP.veh);
        assertEquals("2018-04-05T17:38:36Z", hfp.VP.tst);
        assertTrue(1522949916 == hfp.VP.tsi);
        assertTrue(0.16 - hfp.VP.spd < 0.00001f);
        assertTrue(225 == hfp.VP.hdg);
        assertTrue(60.194481 - hfp.VP.lat < 0.00001f);
        assertTrue(25.03095 - hfp.VP.longitude < 0.00001f);
        assertTrue(0 == hfp.VP.acc);
        assertTrue(-25 == hfp.VP.dl);
        assertTrue(2819 - hfp.VP.odo < 0.00001f);
        assertTrue(0 == hfp.VP.drst);
        assertEquals(java.sql.Date.valueOf("2018-04-05"), hfp.VP.oday);
        assertTrue(636 == hfp.VP.jrn);
        assertTrue(112 == hfp.VP.line);
        assertEquals("20:25", hfp.VP.start);
    }

    @Test
    public void parseTopic() throws Exception {
        HfpMetadata meta = parseAndValidateTopic("/hfp/v1/journey/ongoing/bus/0022/00854/4555B/2/Leppävaara/19:56/4150264/5/60;24/28/65/06");
        assertEquals(HfpMetadata.JourneyType.journey, meta.journey_type);
        assertEquals(true, meta.is_ongoing);
        assertEquals(HfpMetadata.TransportMode.bus, meta.mode.get());
        assertEquals(22, meta.owner_operator_id);
        assertEquals(854, meta.vehicle_number);
        assertEquals(MessageParser.createUniqueVehicleId(22, 854), meta.unique_vehicle_id);

        assertEquals("4555B", meta.route_id.get());
        assertEquals(2, (int)meta.direction_id.get());
        assertEquals("Leppävaara", meta.headsign.get());
        assertEquals(LocalTime.of(19, 56), meta.journey_start_time.get());
        assertEquals("4150264", meta.next_stop_id.get());
        assertEquals(5, (int)meta.geohash_level.get());

    }

    private HfpMetadata parseAndValidateTopic(String topic) throws Exception {
        OffsetDateTime now = OffsetDateTime.now();
        Optional<HfpMetadata> maybeMeta = MessageParser.parseMetadata(topic, now);
        assertTrue(maybeMeta.isPresent());
        HfpMetadata meta = maybeMeta.get();
        assertEquals(now, meta.received_at);
        assertEquals("v1", meta.topic_version);
        return meta;
    }

    @Test
    public void testTopicPrefixParsing() throws Exception {
        String prefix = parseTopicPrefix("/hfp/v1/journey/ongoing/bus/0022/00854/4555B/2/Leppävaara/19:56/4150264/5/60;24/28/65/06");
        assertEquals("/hfp/", prefix);
        String emptyPrefix = parseTopicPrefix("/v1/journey/ongoing/bus/0022/00854/4555B/2/Leppävaara/19:56/4150264/5/60;24/28/65/06");
        assertEquals("/", emptyPrefix);
        String longerPrefix = parseTopicPrefix("/hsldevcomm/public/hfp/v1/journey/ongoing/bus/0022/00854/4555B/2/Leppävaara/19:56/4150264/5/60;24/28/65/06");
        assertEquals("/hsldevcomm/public/hfp/", longerPrefix);

    }

    private String parseTopicPrefix(String topic) throws Exception {
        final String[] allParts = topic.split("/");
        int versionIndex = MessageParser.findVersionIndex(allParts);
        return MessageParser.joinFirstNParts(allParts, versionIndex, "/");
    }
}

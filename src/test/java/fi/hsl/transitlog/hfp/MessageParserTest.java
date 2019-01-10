package fi.hsl.transitlog.hfp;

import org.junit.Test;

import java.net.URL;
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
}

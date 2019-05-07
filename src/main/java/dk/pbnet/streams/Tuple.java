/*
 * Poul Møller Hansen <ph@pbnet.dk> (2019)
 * Created on 27. apr. 2019
 */
package dk.pbnet.streams;

import com.google.gson.Gson;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.common.serialization.Serializer;

/**
 *
 * @author pmh
 */
public class Tuple implements Serializer {

    UUID ident;
    Long epoch;
    Integer lon;
    Integer lat;

    private static final Charset CHARSET = Charset.forName("UTF-8");
    private static Gson gson = new Gson();

    public Tuple() {
    }

    public Tuple(UUID ident, Long epoch, Integer lon, Integer lat) {
        this.ident = ident;
        this.epoch = epoch;
        this.lon = lon;
        this.lat = lat;
    }

    public UUID getIdent() {
        return ident;
    }

    public Long getEpoch() {
        return epoch;
    }

    public Integer getLon() {
        return lon;
    }

    public Integer getLat() {
        return lat;
    }

    @Override
    public void configure(Map map, boolean bln) {

    }

    @Override
    public byte[] serialize(String string, Object t) {
        String line = gson.toJson(t);
        return line.getBytes(CHARSET);
    }

    @Override
    public void close() {

    }

}

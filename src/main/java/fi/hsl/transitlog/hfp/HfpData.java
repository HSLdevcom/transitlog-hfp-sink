package fi.hsl.transitlog.hfp;

public class HfpData {
    private final HfpMetadata metadata;
    private final HfpMessage payload;

    public HfpData(HfpMetadata metadata, HfpMessage payload) {
        this.metadata = metadata;
        this.payload = payload;
    }

    public HfpMetadata getMetadata() {
        return metadata;
    }

    public HfpMessage getPayload() {
        return payload;
    }
}

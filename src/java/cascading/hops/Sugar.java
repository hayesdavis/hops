package cascading.hops;

import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;

public class Sugar {

    public static Fields $(String... fields) {
        return new Fields(fields);
    }

    public static Tap tsv(Fields fields, String path) {
        return Taps.hfs(Schemes.tsv(fields), path);
    }

    public static Tap tsv(Fields fields, String path, SinkMode mode) {
        return Taps.hfs(Schemes.tsv(fields), path, mode);
    }

    public static Tap textLine(Fields fields, String path) {
        return Taps.hfs(Schemes.textLine(fields), path);
    }

}

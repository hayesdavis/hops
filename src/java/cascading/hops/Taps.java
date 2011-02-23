package cascading.hops;

import cascading.scheme.Scheme;
import cascading.tap.Hfs;
import cascading.tap.SinkMode;
import cascading.tap.Tap;

public class Taps {

    public static Tap hfs(Scheme scheme, String path) {
        return new Hfs(scheme,path);
    }
    
    public static Tap hfs(Scheme scheme, String path, SinkMode mode) {
        return new Hfs(scheme,path,mode);
    }
    
}

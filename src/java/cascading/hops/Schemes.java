package cascading.hops;

import cascading.scheme.Scheme;
import cascading.scheme.TextDelimited;
import cascading.tuple.Fields;

public class Schemes {

    public static Scheme tsv(Fields fields) {
        return new TextDelimited(fields,"\t");
    }
    
    public static Scheme csv(Fields fields) {
        return new TextDelimited(fields,",");
    }
    
}

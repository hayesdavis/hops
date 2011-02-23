package cascading.hops;

import cascading.operation.Filter;
import cascading.operation.filter.FilterNotNull;
import cascading.operation.filter.FilterNull;

public class Filters {

    public static Filter notNull() {
        return new FilterNotNull();
    }
    
    public static Filter isNull() {
        return new FilterNull();
    }
    
}

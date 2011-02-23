package cascading.hops;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Aggregator;
import cascading.operation.Filter;
import cascading.operation.Identity;
import cascading.operation.expression.ExpressionFilter;
import cascading.operation.filter.Not;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Unique;
import cascading.tap.Tap;
import cascading.tuple.Fields;

public class Query {

    private Tap source;
    private Tap sink;
    private Pipe assembly;
    
    public Query() {
        this("hops_query_"+System.currentTimeMillis());
    }
    
    public Query(String name) {
        this.assembly = new Pipe(name);
    }
    
    public Query apply(Class<? extends Pipe> op, Object... opArgs) {
        Object[] args = new Object[opArgs.length+1];
        args[0] = this.assembly;
        System.arraycopy(opArgs,0,args,1,opArgs.length);
        try {
            this.assembly = Reflection.newInstance(op,args);
        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return this;
    }

    public Query select(Fields fields) {
        each(fields,new Identity());
        return this;
    }
    
    public Query unique(Fields fields) {
        return this.apply(Unique.class,fields);
    }
    
    public Query each(Object... args) {
        return apply(Each.class,args);
    }
    
    public Query every(Object... args) {
        return apply(Every.class,args);
    }
    
    // Grouping and aggregation
    public Query groupBy(Fields group) {
        return apply(GroupBy.class,group);
    }
    
    public Query aggregate(Fields group, Aggregator aggregator) {
        return this.groupBy(group).every(aggregator);
    }
    
    // Filtering
    public Query filter(Object... args) {
        return each(args);
    }
    
    public Query filter(String expression, Class paramType) {
        return filter(new ExpressionFilter(expression,paramType));
    }
    
    public Query where(Object... args) {
        for(int i=0; i<args.length; i++) {
            if(args[i] instanceof Filter) {
                args[i] = new Not((Filter)args[i]);
            }
        }
        return filter(args);
    }
    
    public Query where(String expression, Class paramType) {
        return where(new ExpressionFilter(expression,paramType));
    }    
    
    // Loading/Storing
    public Query load(Tap tap) {
        this.source = tap;
        return this;
    }
    
    public Query store(Tap tap) {
        this.sink = tap;
        return this;
    }
    
    public Query from(Tap tap) {
        return this.load(tap);
    }
    
    public Query into(Tap tap) {
        return this.store(tap);
    }
    
    // Flows
    public Flow toFlow() {
        return new FlowConnector().connect(this.source, this.sink, this.assembly);
    }
    
    public void complete() {
        toFlow().complete();
    }
}

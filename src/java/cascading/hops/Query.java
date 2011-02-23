package cascading.hops;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

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
    private Query parent;
    private Pipe assembly;
    
    public Query() {
        this("hops_query_"+System.currentTimeMillis());
    }
    
    public Query(String name) {
        this.assembly = new Pipe(name);
    }
    
    protected Query(Query parent, String name) {
        this.parent = parent;
        this.assembly = new Pipe(name,parent.assembly);
    }
    
    public Query apply(Class<? extends Pipe> op, Object... opArgs) {
        Object[] args = new Object[opArgs.length+1];
        args[0] = this.assembly;
        System.arraycopy(opArgs,0,args,1,opArgs.length);
        Class[] paramTypes = new Class[args.length];
        for(int i=0; i<args.length; i++) {
            paramTypes[i] = args[i].getClass();
        }
        try {
            Constructor<? extends Pipe> cons = this.getConstructor(op,paramTypes);
            this.assembly = cons.newInstance(args); 
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
    
    public Query groupBy(Fields group) {
        return apply(GroupBy.class,group);
    }
    
    public Query aggregate(Fields group, Aggregator aggregator) {
        return this.groupBy(group).every(aggregator);
    }
    
    /* Filtering */
    public Query filter(Fields fields, Filter filter) {
        return each(fields,filter);
    }
    
    public Query filter(Fields fields, String expression, Class paramType) {
        return filter(fields,new ExpressionFilter(expression,paramType));
    }
    
    public Query where(Fields fields, Filter filter) {
        return filter(fields,new Not(filter));
    }
    
    public Query where(Fields fields, String expression, Class paramType) {
        return where(fields,new ExpressionFilter(expression,paramType));
    }
    
    // Splitting
    public Query splitOne(String name) {
        return new Query(this,name);
    }
    
    public List<Query> split(String... names) {
        List<Query> splits = new ArrayList<Query>();
        for(String name : names) {
            splits.add(this.splitOne(name));
        }
        return splits;
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
    
    public Flow toFlow() {
        return new FlowConnector().connect(this.source, this.sink, this.assembly);
    }
    
    public void complete() {
        toFlow().complete();
    }
    
    private Constructor getConstructor(Class type, Class[] params) throws Exception {
        for(Constructor cons : type.getConstructors()) {
            if(this.isMatch(cons.getParameterTypes(),params)) {
                return cons;
            }
        }
        return null;
    }
    
    private boolean isMatch(Class[] declaredTypes, Class[] argTypes) {
        if(declaredTypes.length != argTypes.length) {
            return false;
        }
        for(int i=0; i<declaredTypes.length; i++) {
            if(!declaredTypes[i].isAssignableFrom(argTypes[i])) {
                return false;
            }
        }
        return true;
    }
}

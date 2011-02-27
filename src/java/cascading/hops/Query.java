package cascading.hops;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Aggregator;
import cascading.operation.Filter;
import cascading.operation.Identity;
import cascading.operation.expression.ExpressionFilter;
import cascading.operation.filter.Not;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.CountBy;
import cascading.pipe.assembly.SumBy;
import cascading.pipe.assembly.Unique;
import cascading.pipe.cogroup.Joiner;
import cascading.tap.Tap;
import cascading.tuple.Fields;

public class Query {

    private Tap source;
    private Tap sink;
    private Pipe assembly;
    
    private Query parent;
    private List<Query> children;
    
    public Query() {
        this("hops_query_"+System.currentTimeMillis());
    }
    
    public Query(String name) {
        this(null,name);
    }
    
    private Query(Query parent, String name) {
        this.setParent(parent);
        if(parent == null) {
            this.assembly = new Pipe(name);
        } else {
            this.assembly = new Pipe(name,parent.assembly);
            this.getParent().getChildren().add(this);
        }
        this.setChildren(new ArrayList<Query>());
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
    
    public Query countBy(Fields group, Fields countField) {
        return this.apply(CountBy.class,group,countField);
    }
    
    public Query countBy(Fields group) {
        return this.countBy(group,new Fields("count"));
    }
    
    public Query sumBy(Fields group, Fields valueField, Fields sumField) {
        return this.apply(SumBy.class,group,valueField,sumField,long.class);
    }
    
    public Query sumBy(Fields group, Fields valueField) {
        return this.sumBy(group, valueField, new Fields(valueField.get(0)
            + "_sum"));
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
    
    // Sources/Sinks
    public Query load(Tap tap) {
        this.setSource(tap);
        return this;
    }

    public Query load(Query query) {
        return this.load(query.getSink());
    }
    
    public Query store(Tap tap) {
        this.setSink(tap);
        return this;
    }
    
    public Query from(Tap tap) {
        return this.load(tap);
    }
    
    public Query from(Query query) {
        return this.from(query.getSink());
    }
    
    public Query into(Tap tap) {
        return this.store(tap);
    }

    public void setSource(Tap source) {
        this.source = source;
    }

    public Tap getSource() {
        return source;
    }

    public void setSink(Tap sink) {
        this.sink = sink;
    }

    public Tap getSink() {
        return sink;
    }    
    
    // Splitting and merging
    public Query query(String name) {
        return new Query(this,name);
    }
    
    public Query end() {
        return this.getParent();
    }
    
    public Query merge(Fields on) {
        Pipe[] pipes = new Pipe[getChildren().size()];
        int idx = 0;
        for(Iterator<Query> i = getChildren().iterator(); i.hasNext(); idx++) {
            pipes[idx] = i.next().assembly;
        }
        this.assembly = new GroupBy(pipes,on);
        return this;
    }
    
    public Query join(Fields[] groupOn, Fields output, Joiner joiner) {
        Pipe[] pipes = new Pipe[getChildren().size()];
        int idx = 0;
        for(Iterator<Query> i = getChildren().iterator(); i.hasNext(); idx++) {
            pipes[idx] = i.next().assembly;
        }
        this.assembly = new CoGroup(pipes,groupOn,output,joiner);
        return this;
    }
    
    public Query join(Fields groupOn, Fields output, Joiner joiner) {
        Fields[] groupFields = new Fields[this.getChildren().size()];
        Arrays.fill(groupFields,groupOn);
        return this.join(groupFields,output,joiner);
    }
    
    protected void setChildren(List<Query> children) {
        this.children = children;
    }

    public List<Query> getChildren() {
        return children;
    }

    protected void setParent(Query parent) {
        this.parent = parent;
    }

    public Query getParent() {
        return parent;
    }

    // Flows
    public Flow toFlow() {
        FlowConnector connector = new FlowConnector();
        Flow flow = null;
        if(this.getChildren().isEmpty()) {
            flow = connector.connect(this.getSource(), this.getSink(),
                this.assembly);
        } else {
            Map<String,Tap> sources = new HashMap<String, Tap>();
            Map<String,Tap> sinks = new HashMap<String, Tap>();
            List<Pipe> tails = new ArrayList<Pipe>();
            this.addToFlow(sources,sinks,tails);
            flow = connector.connect(sources, sinks,
                tails.toArray(new Pipe[tails.size()]));
        }
        return flow;
    }

    protected void addToFlow(Map<String,Tap> sources, Map<String,Tap> sinks, List<Pipe> tails) {
        for(Query child : this.getChildren()){
            child.addToFlow(sources,sinks,tails);
        }
        if(this.getSource() != null) {
            for(Pipe head : this.assembly.getHeads()) {
                sources.put(head.getName(),this.getSource());
            }
        }
        if(this.getSink() != null) {
            tails.add(this.assembly);
            sinks.put(this.assembly.getName(),this.getSink());
        }
    }
    
    public void complete() {
        toFlow().complete();
    }
}

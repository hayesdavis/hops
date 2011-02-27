package cascading.hops;

import java.util.ArrayList;
import java.util.List;

import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;

public class QueryCascade {
    
    public static void complete(Query... newQueries) {
        new QueryCascade().add(newQueries).complete();
    }
    
    private List<Query> queries;
    
    public QueryCascade() {
        this.queries = new ArrayList<Query>();
    }
    
    public QueryCascade add(Query... newQueries) {
        for(Query query : newQueries) {
            this.queries.add(query);            
        }
        return this;
    }
    
    public Cascade toCascade() {
        List<Flow> flows = new ArrayList<Flow>();
        for(Query query : this.queries) {
            flows.add(query.toFlow());
        }
        return new CascadeConnector().connect(flows);        
    }
    
    public void complete() {
        this.toCascade().complete();
    }

}

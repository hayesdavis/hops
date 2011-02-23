package cascading.hops.examples;

import static cascading.hops.Sugar.$;
import static cascading.hops.Sugar.textLine;
import static cascading.hops.Sugar.tsv;
import cascading.hops.Query;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexGenerator;
import cascading.tap.SinkMode;

public class WordCount {

    /**
     * Based on the example here: 
     *  http://www.cascading.org/1.2/userguide/html/ch02.html 
     * 
     * but with an extra check to only include words seen more than once.
     */
    public static void main(String[] args) {    
        String regex = "(?<!\\pL)(?=\\pL)[^ ]*(?<=\\pL)(?!\\pL)";
        Query q = new Query("word-count");
        q.from(textLine($("line"),args[0]))
            .each($("line"),new RegexGenerator($( "word" ),regex))
            .aggregate($("word"),new Count($("count")))
            .where("count > 1",Long.class)
            .store(tsv($("word","count"),args[1],SinkMode.REPLACE));
        q.complete();
    }

}

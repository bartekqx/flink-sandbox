package examples.common;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class PeopleLocationSplitter implements MapFunction<String, Tuple2<Long, String>> {

    @Override
    public Tuple2<Long, String> map(String s) throws Exception {
        final String[] splitted = s.split(",");
        return Tuple2.of(Long.valueOf(splitted[0]), splitted[1]);
    }

}

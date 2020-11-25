package examples.batching;

import examples.common.PeopleLocationSplitter;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class FullOuterJoinExample {
    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final String peopleFile = args[0];
        final String locationFile = args[1];
        final String outputFile = args[2] + "_" + System.currentTimeMillis();

        final DataSet<Tuple2<Long, String>> people = env.readTextFile(peopleFile)
                .map(new PeopleLocationSplitter());

        final DataSet<Tuple2<Long, String>> locations = env.readTextFile(locationFile)
                .map(new PeopleLocationSplitter());

        people.fullOuterJoin(locations)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Long, String>, Tuple2<Long, String>, Tuple3<Long, String, String>>() {
                    @Override
                    public Tuple3<Long, String, String> join(Tuple2<Long, String> person, Tuple2<Long, String> location) throws Exception {
                        if (location == null) {
                            return Tuple3.of(person.f0, person.f1, "null");
                        } else if (person == null) {
                            return Tuple3.of(location.f0, "null", location.f1);
                        }
                        return Tuple3.of(person.f0, person.f1, location.f1);
                    }
                })
                .writeAsCsv(outputFile);

        env.execute();
    }
}

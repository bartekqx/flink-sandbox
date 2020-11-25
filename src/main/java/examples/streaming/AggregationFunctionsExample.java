package examples.streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AggregationFunctionsExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final String inputFile = args[0];

        DataStream<Tuple3<String, String, Long>> splitted = env.readTextFile(inputFile)
                .map(new MapFunction<String, Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> map(String s) throws Exception {
                        final String[] splitted = s.split(",");
                        return Tuple3.of(splitted[0], splitted[1], Long.parseLong(splitted[2]));
                    }
                });

        splitted.keyBy(0).sum(2).writeAsText("/home/user/IdeaProjects/flink-sandbox/outputs/sum.txt");
        splitted.keyBy(0).min(2).writeAsText("/home/user/IdeaProjects/flink-sandbox/outputs/min.txt");
        splitted.keyBy(0).minBy(2).writeAsText("/home/user/IdeaProjects/flink-sandbox/outputs/minBy.txt");
        splitted.keyBy(0).max(2).writeAsText("/home/user/IdeaProjects/flink-sandbox/outputs/max.txt");
        splitted.keyBy(0).maxBy(2).writeAsText("/home/user/IdeaProjects/flink-sandbox/outputs/maxBy.txt");

        env.execute();
    }
}

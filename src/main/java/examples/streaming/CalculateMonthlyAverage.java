package examples.streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CalculateMonthlyAverage {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final String inputFile = args[0];
        
        env.readTextFile(inputFile)
                .map(new MapFunction<String, Tuple3<String, Long, Long>>() {
                    @Override
                    public Tuple3<String, Long, Long> map(String s) throws Exception {
                        final String[] splitted = s.split(",");
                        return Tuple3.of(splitted[0], Long.parseLong(splitted[1]), 1L);
                    }
                })
                .keyBy(0)
                .reduce(new ReduceFunction<Tuple3<String, Long, Long>>() {
                    @Override
                    public Tuple3<String, Long, Long> reduce(Tuple3<String, Long, Long> previous,
                                                             Tuple3<String, Long, Long> current) throws Exception {
                        return Tuple3.of(previous.f0, previous.f1 + current.f1, previous.f2 + current.f2);
                    }
                })
                .map(new MapFunction<Tuple3<String, Long, Long>, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(Tuple3<String, Long, Long> in) throws Exception {
                        return Tuple2.of(in.f0, (in.f1 * 1.0) / (in.f2 * 1.0));
                    }
                })
                .print();

        env.execute();
    }
}

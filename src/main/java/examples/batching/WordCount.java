package examples.batching;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

public class WordCount {
    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final String inputFile = args[0];
        final String outputFile = args[1] + "_" + System.currentTimeMillis();

        env.readTextFile(inputFile)
                .filter(word -> word.startsWith("D"))
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String s) throws Exception {
                        return Tuple2.of(s, 1L);
                    }
                })
                .groupBy(0)
                .sum(1)
                .writeAsCsv(outputFile);

        env.execute();
    }
}

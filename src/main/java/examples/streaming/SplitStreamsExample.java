package examples.streaming;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SplitStreamsExample {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final DataStream<Integer> numbers = env.readTextFile(args[0])
                .map(Integer::parseInt);

        numbers.print();

        final OutputTag<Integer> even = new OutputTag<Integer>("even-output"){};
        final OutputTag<Integer> odd = new OutputTag<Integer>("odd-output"){};

        final SingleOutputStreamOperator<Integer> mainDataStream = numbers
                .process(new ProcessFunction<Integer, Integer>() {
                    @Override
                    public void processElement(Integer integer, Context context, Collector<Integer> collector) throws Exception {
                        collector.collect(integer);
                        if (integer % 2 == 0) {
                            context.output(even, integer);
                        } else {
                            context.output(odd, integer);
                        }
                    }
                });

        mainDataStream.getSideOutput(even)
                .writeAsText("/home/user/IdeaProjects/flink-sandbox/outputs/even.txt");
        mainDataStream.getSideOutput(odd)
                .writeAsText("/home/user/IdeaProjects/flink-sandbox/outputs/odd.txt");

        env.execute();
    }
}

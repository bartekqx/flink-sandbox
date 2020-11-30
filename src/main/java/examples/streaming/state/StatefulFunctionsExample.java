package examples.streaming.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.Int;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;


public class StatefulFunctionsExample {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("localhost", 9090, "|")
                .map(new MapFunction<String, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> map(String s) throws Exception {
                        String[] splitted = s.split(",");
                        return Tuple2.of(Integer.parseInt(splitted[0]), Integer.parseInt(splitted[1]));
                    }
                })
                .keyBy(0)
                .flatMap(new MyMapFunction())
                .print();

        env.execute();
    }

    static class MyMapFunction extends RichFlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

        private transient ListState<Integer> listSum;
        private transient ValueState<Long> counter;

        @Override
        public void flatMap(Tuple2<Integer, Integer> input, Collector<Tuple2<Integer, Integer>> collector) throws Exception {
            Long currCounter = counter.value();

            currCounter += 1;
            listSum.add(input.f1);

            if (currCounter >= 10) {
                final Integer sum = StreamSupport.stream(listSum.get().spliterator(), false)
                        .mapToInt(Integer::intValue)
                        .sum();
                collector.collect(Tuple2.of(input.f0, sum));
                listSum.clear();
                counter.clear();
            }

            counter.update(currCounter);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            final ListStateDescriptor<Integer> sumDescriptor = new ListStateDescriptor<Integer>("sum", Types.INT);
            listSum = getRuntimeContext().getListState(sumDescriptor);
            final ValueStateDescriptor<Long> counterDescriptor = new ValueStateDescriptor<Long>("counter", Types.LONG, 0L);
            counter = getRuntimeContext().getState(counterDescriptor);
        }
    }
}

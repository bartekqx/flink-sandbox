package examples.streaming.evictor;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class EvictorExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final DataStream<String> data = env.socketTextStream("localhost", 9090, "|");

        DataStream<Tuple3<Long,String,Long>> counted = data
                .map(new MapFunction<String, Tuple3<Long, String, Long>>() {
                    @Override
                    public Tuple3<Long, String, Long> map(String s) throws Exception {
                        String[] splitted = s.split(",");
                        return Tuple3.of(Long.parseLong(splitted[0]), splitted[1], 1L);
                    }
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<Long, String, Long>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple3<Long, String, Long> tuple) {
                        return tuple.f0;
                    }
                })
                .keyBy(1)
                .window(TumblingEventTimeWindows.of(Time.seconds(2)))
                .evictor(CountEvictor.of(1))
                .reduce(new ReduceFunction<Tuple3<Long, String, Long>>() {
                    @Override
                      public Tuple3<Long, String, Long> reduce(Tuple3<Long, String, Long> t1,
                                                             Tuple3<Long, String, Long> t2) throws Exception {
                        final long timestamp = System.currentTimeMillis();
                        return Tuple3.of(timestamp, t1.f1, t1.f2 + t2.f2);
                    }
                });
        counted.print();


        env.execute("Evictor");
    }

}

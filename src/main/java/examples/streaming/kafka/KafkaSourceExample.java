package examples.streaming.kafka;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class KafkaSourceExample {

    private static final String TOPIC_NAME = "test_topic";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        env.addSource(new FlinkKafkaConsumer011<>(TOPIC_NAME, new SimpleStringSchema(), properties))
                .map(new MapFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        return Tuple2.of(s, 1);
                    }
                })
                .keyBy(0)
                .sum(1)
                .print();

        env.execute();
    }
}

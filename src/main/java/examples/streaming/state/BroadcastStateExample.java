package examples.streaming.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class BroadcastStateExample {

    private static final MapStateDescriptor<String, String> excludeEmployeesDescriptor =
            new MapStateDescriptor<String, String>("exclude_employees", Types.STRING, Types.STRING);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = configureEnv();

        final DataStream<String> excludedEmployees = env.readTextFile(System.getProperty("user.dir") + "/inputs/dismissed-employees.txt");

        final BroadcastStream<String> excludedEmpBroadcast = excludedEmployees.broadcast(excludeEmployeesDescriptor);

        env.readTextFile( System.getProperty("user.dir") + "/inputs/employees.txt")
                .map(new MapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(String s) throws Exception {
                        String[] splitted = s.split(",");

                        return Tuple2.of(splitted[0], splitted[1]);
                    }
                })
                .keyBy(1)
                .connect(excludedEmpBroadcast)
                .process(new ExcludeEmployeesCalcFunction(excludeEmployeesDescriptor))
                .writeAsText(System.getProperty("user.dir") + "/outputs/" + "excluded-employees_" + System.currentTimeMillis());

        env.execute();
    }

    private static class ExcludeEmployeesCalcFunction
            extends KeyedBroadcastProcessFunction<String, Tuple2<String, String>, String, Tuple2<String,Integer>> {

        private ValueState<Integer> excludedEmployeesCounter;
        private final MapStateDescriptor<String, String> excludeEmployeesDescriptor;

        public ExcludeEmployeesCalcFunction(MapStateDescriptor<String, String> excludeEmployeesDescriptor) {
            this.excludeEmployeesDescriptor = excludeEmployeesDescriptor;
        }

        @Override
        public void processElement(Tuple2<String, String> value, ReadOnlyContext readOnlyContext, Collector<Tuple2<String, Integer>> collector) throws Exception {
            Integer currCounter = excludedEmployeesCounter.value();
            final String empId = value.f0;

            if (readOnlyContext.getBroadcastState(excludeEmployeesDescriptor).contains(empId)) {
                currCounter++;
                excludedEmployeesCounter.update(currCounter);
                collector.collect(Tuple2.of(value.f1, currCounter));
            }
        }

        @Override
        public void processBroadcastElement(String empData, Context context, Collector<Tuple2<String, Integer>> collector) throws Exception {
            final String id = empData.split(",")[0];
            context.getBroadcastState(excludeEmployeesDescriptor).put(id, empData);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            final ValueStateDescriptor<Integer> desc = new ValueStateDescriptor<Integer>("counter", Types.INT, 0);
            excludedEmployeesCounter = getRuntimeContext().getState(desc);
        }
    }

    private static StreamExecutionEnvironment configureEnv() {
        final Configuration conf = new Configuration();
        conf.setString("state.backend", "filesystem");
        conf.setString("state.savepoints.dir", "file:///tmp/savepoints");
        conf.setString("state.checkpoints.dir", "file:///tmp/checkpoints");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 100));

        return env;
    }
}

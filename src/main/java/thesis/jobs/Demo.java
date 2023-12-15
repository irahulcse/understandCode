package thesis.jobs;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import thesis.common.sources.PolicyCreator;
import thesis.common.sources.VehicleContextSource;
import thesis.context.VehicleContext;
import thesis.demo.DemoFrame;
import thesis.demo.NewDemoSink;
import thesis.flink.Descriptors;
import thesis.policy.Policy;
import thesis.util.PipelineUtil;

import java.io.IOException;
import java.util.Map;

/**
 * The demonstration example during the final presentation.
 * How to change the configurations:
 * 1. adjust the csv-file of policy definition
 * 2. adjust the number of apps.
 * 3. add or delete the code snippet.
 * Notice that the names of the apps in the main method should match with the names in the policy csv-file.
 */
public class Demo {
    private static final int frequency = 10; // Refresh rate of the image.
    private static final DemoFrame demoFrame;

    static {
        try {
            demoFrame = new DemoFrame(frequency);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static final NewDemoSink demoSink = new NewDemoSink(demoFrame, frequency, 2); // Adjust the number of apps here
    private static final Map<String, Integer> parallelismMap = Map.of("image", 2, "speed",1, "location",1); // Adjust the parallelism here. Keys are data section names, values are the number of parallel instances.

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<VehicleContext> contextStream = env.addSource(new VehicleContextSource(frequency));
        BroadcastStream<Policy> policyBroadcastStream = env.fromElements(PolicyCreator.generatePolicy("demoPolicy")).broadcast(Descriptors.policyStateDescriptor);

        String appName1 = "app1";
        DataStream<Tuple2<String, VehicleContext>> resultApp1 = PipelineUtil.constructPipeline(appName1, parallelismMap, env, contextStream, policyBroadcastStream);
        String appName2 = "app2";
        DataStream<Tuple2<String, VehicleContext>> resultApp2 = PipelineUtil.constructPipeline(appName2, parallelismMap, env, contextStream, policyBroadcastStream);

        resultApp1.union(resultApp2).addSink(demoSink);
        //resultApp1.addSink(demoSink);

        env.execute();
    }


}

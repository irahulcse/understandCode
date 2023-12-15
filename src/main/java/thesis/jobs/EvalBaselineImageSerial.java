package thesis.jobs;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import thesis.common.GlobalConfig;
import thesis.common.sources.CameraSource;
import thesis.common.sources.PolicyCreator;
import thesis.context.VehicleContext;
import thesis.context.data.ImageData;
import thesis.demo.FlatMapper;
import thesis.flink.Selector;
import thesis.flink.SimpleStatefulSerialPETEnforcement;
import thesis.flink.Descriptors;
import thesis.flink.FilterByDataSection;
import thesis.flink.SituationEvaluatorFromContext;
import thesis.flink.SwitchingDecision;
import thesis.policy.Policy;
import thesis.util.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
/**
 * <p>The evaluation job for Variant 1 pipeline</p>
 * <p>In order to add PETs to the pipeline, refer to the following code snippet,
 * and add the corresponding PET path to your target split stream.
 * <pre>
 *     // The PET you would like to add is in the path {@code petPath1}
 *     private static final String petPath1 = GlobalConfig.textInputSource + "/petDescriptions/imagePET1.json";
 *     // The target stream is the image, whose corresponding split is {@code imageSplit}
 *     // Use the correct class type in the argument of {@link SimpleStatefulSerialPETEnforcement}
 *     {@code DataStream<Tuple2<String, VehicleContext>>} processedPET1 = imageSplit.process(new SimpleStatefulSerialPETEnforcement<>(ImageData.class, readPETDescription(petPath1)));
 * </pre></p>
 * <p>Components for evaluation are {@link OutputTool} as well as a timer task that measures {@link MemoryUsage}</p>
 *
 */
public class EvalBaselineImageSerial {

    private static final String petPath1 = GlobalConfig.textInputSource + "/petDescriptions/imagePET1.json";
    private static final String petPath2 = GlobalConfig.textInputSource + "/petDescriptions/imagePET2.json";
    private static final String petPath3 = GlobalConfig.textInputSource + "/petDescriptions/imagePET3.json";

    private static final Timer timer = new Timer();

    public static OutputTool outputTool;

    static {
        try {
            outputTool = new OutputTool();
            Runtime.getRuntime().addShutdownHook(outputTool.complete(false));
        } catch (IOException e) {
            System.out.println("Error initializing output tool.");
        }
    }
    public static Map<Integer, UUID> triggerIDMapping = new HashMap<>();
    public static void main(String[] args) throws Exception {

        startMemoryLogger();
        for (int i = 0; i < 110; i++) {
            triggerIDMapping.put(i, UUID.randomUUID());
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<ImageData> imageStream = env.addSource(new CameraSource(10,1000));

        BroadcastStream<Policy> policyBroadcastStream = env.fromElements(PolicyCreator.generatePolicy("statefulSpeedPolicy")).broadcast(Descriptors.policyStateDescriptor); // Which policy doesn't matter, since the trigger signal is simulated.

        DataStream<Tuple2<String, VehicleContext>> contextOnlyScalar = imageStream.flatMap(new FlatMapper<>());

        SingleOutputStreamOperator<SwitchingDecision> decisionStream = contextOnlyScalar.keyBy((KeySelector<Tuple2<String, VehicleContext>, String>) value -> value.f0).
                connect(policyBroadcastStream).process(new SituationEvaluatorSimulatorUpgrade(triggerIDMapping, petPath1));

        DataStream<Tuple2<String, VehicleContext>> forkedVehicleContextRecords = decisionStream.getSideOutput(SituationEvaluatorFromContext.contextOutputTag);
        // Split. Otherwise, error by pushing to operator when emitting from side output.
        DataStream<Tuple2<String, VehicleContext>> imageSplit = forkedVehicleContextRecords.filter(new FilterByDataSection("image"));
        // Adjust the comments according to the number of participating PETs. Don't forget to union them if multiple PETs are used.
        DataStream<Tuple2<String, VehicleContext>> processedPET1 = imageSplit.process(new SimpleStatefulSerialPETEnforcement<>(ImageData.class, readPETDescription(petPath1)));
/*        DataStream<Tuple2<String, VehicleContext>> processedPET2 = imageSplit.process(new SimpleStatefulSerialPETEnforcement<>(ImageData.class, readPETDescription(petPath2)));
        DataStream<Tuple2<String, VehicleContext>> processedPET3 = imageSplit.process(new SimpleStatefulSerialPETEnforcement<>(ImageData.class, readPETDescription(petPath3)));

        //DataStream<Tuple2<String, VehicleContext>> union = processedPET1.union(processedPET2).union(processedPET3);*/
        // If the result is unioned, use the unioned stream below.
        DataStream<ImageData> processedResult = processedPET1.connect(decisionStream).process(new Selector<>("image", 1, ImageData.class), TypeInformation.of(ImageData.class));
        processedResult.addSink(new SinkFunction<>() {
            @Override
            public void invoke(ImageData value, Context context) throws Exception {
                OutputTool.log(value, "");
            }
        });
        env.execute();

    }

    public static String readPETDescription(String path) throws IOException {
        try {
            return new String(Files.readAllBytes(Paths.get(path)));
        } catch (IOException e) {
            throw new IOException(e.getCause());
        }
    }

    private static void startMemoryLogger() {
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                long freeMem = Runtime.getRuntime().freeMemory();
                long maxMem = Runtime.getRuntime().maxMemory();
                long totalMem = Runtime.getRuntime().totalMemory();
                try {
                    OutputTool.log(new MemoryUsage(System.currentTimeMillis(), maxMem, totalMem, freeMem), "");
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }, 2000, 1000);
    }


}

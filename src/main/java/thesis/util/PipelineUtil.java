package thesis.util;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import thesis.common.GlobalConfig;
import thesis.context.VehicleContext;
import thesis.context.data.Data;
import thesis.context.data.ImageData;
import thesis.context.data.LocationData;
import thesis.context.data.ScalarData;
import thesis.demo.VehicleContextReconstructor;
import thesis.flink.*;
import thesis.pet.StatefulSerialPETEnforcement;
import thesis.pet.StatefulSerialPETEnforcementUpgrade;
import thesis.pet.StatelessParallelPETEnforcement;
import thesis.policy.Policy;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * A class containing several methods to build the pipeline for multiple purposes.
 */
public class PipelineUtil {
    /**
     * Contruct the complete pipeline for situation evaluation and PET enforcement for a querying application.
     *
     * @param appName               The name of the querying application
     * @param parallelismMap        A map defining the needed parallelism for processing each data section
     * @param env                   the execution environment
     * @param inputContextStream    the input of the pre-assembled {@link VehicleContext}
     * @param policyBroadcastStream the broadcast stream which delivers the complete policies for each involved querying application
     * @return A {@link Tuple2} instance, made up of the name of the querying application
     * and a {@link DataStream} containing the instances of {@link VehicleContext} with processed records
     */
    public static DataStream<Tuple2<String, VehicleContext>> constructPipeline(String appName, Map<String, Integer> parallelismMap,
                                                                               StreamExecutionEnvironment env,
                                                                               DataStream<VehicleContext> inputContextStream, BroadcastStream<Policy> policyBroadcastStream) {

        KeyedStream<Tuple2<String, VehicleContext>, String> keyedContextStream = inputContextStream.flatMap(new ContextFanOutForKeys()).keyBy((KeySelector<Tuple2<String, VehicleContext>, String>) value -> value.f0);
        SingleOutputStreamOperator<SwitchingDecision> decisionMaker = keyedContextStream.connect(policyBroadcastStream).process(new SituationEvaluatorFromContext(appName)).setParallelism(1);

        BroadcastStream<SwitchingDecision> broadcastDecisions = decisionMaker.broadcast(Descriptors.decisionMapState);
        DataStream<Tuple2<String, VehicleContext>> forkedVehicleContextRecords = decisionMaker.getSideOutput(SituationEvaluatorSimulator.contextOutputTag);

        DataStream<Tuple2<String, VehicleContext>> scalarSplit = forkedVehicleContextRecords.filter(new FilterByDataSection("speed"));
        DataStream<Tuple2<String, VehicleContext>> locationSplit = forkedVehicleContextRecords.filter(new FilterByDataSection("location"));
        DataStream<Tuple2<String, VehicleContext>> imageSplit = forkedVehicleContextRecords.filter(new FilterByDataSection("image"));

        DataStream<Tuple2<String, VehicleContext>> processedSpeed = PipelineUtil.getOrderedOutputStream(env, appName, parallelismMap.get("speed"), "speed", ScalarData.class, false, broadcastDecisions, scalarSplit);
        DataStream<Tuple2<String, VehicleContext>> processedLocation = PipelineUtil.getOrderedOutputStream(env, appName, parallelismMap.get("location"), "location", LocationData.class, false, broadcastDecisions, locationSplit);
        DataStream<Tuple2<String, VehicleContext>> processedImage = PipelineUtil.getOrderedOutputStream(env, appName, parallelismMap.get("image"), "image", ImageData.class, false, broadcastDecisions, imageSplit);

        DataStream<Tuple2<String, VehicleContext>> unionPartialContext = processedSpeed.union(processedLocation).union(processedImage);
        DataStream<Tuple2<String, VehicleContext>> readyToPresent = unionPartialContext.keyBy((KeySelector<Tuple2<String, VehicleContext>, String>) value -> "postprocessing")
                .process(new VehicleContextReconstructor(appName));

/*        processedSpeed.print();
        processedLocation.print();*/

        return readyToPresent;
    }


    /**
     * A method that build a pipeline for a data section that either centrally processed with a {@link StatefulSerialPETEnforcement} operator
     * or distributedly by several parallel instances of {@link StatelessParallelPETEnforcement} operator.
     * The number of parallel instances is given as an input argument.
     *
     * @param parallelism     the number of parallel instances of the desired operator
     * @param broadcastStream the broadcast switching decision
     * @param inputStream     the stream containing the data records to be processed
     * @param env             the stream execution environment
     * @param <T>             Type of the data records
     * @param measure         measure mode
     * @return a processed stream (parallelism=1) or a processed ordered stream (parallelism>1)
     * @throws IllegalArgumentException if the parallelism is less than 1
     */
    public static <T extends Data<?>> DataStream<Tuple2<String, VehicleContext>> getOrderedOutputStream(StreamExecutionEnvironment env,
                                                                                                        String appName,
                                                                                                        int parallelism, String dataSection, Class<T> dataType, boolean measure,
                                                                                                        BroadcastStream<SwitchingDecision> broadcastStream,
                                                                                                        DataStream<Tuple2<String, VehicleContext>> inputStream) throws IllegalArgumentException {
        String kafkaTopic = GlobalConfig.SWITCHING_TOPIC + "-" + appName;
        if (parallelism > 1) {
            StatelessParallelPETEnforcement<T> pet = new StatelessParallelPETEnforcement<>(dataSection, dataType, measure);
            // PET ready signal source
            KafkaSource<Tuple2<Boolean, UUID>> switchingReadySignalInput = KafkaSource.<Tuple2<Boolean, UUID>>builder()
                    .setBootstrapServers(GlobalConfig.BOOTSTRAP_SERVER).setTopics(kafkaTopic)
                    .setGroupId("pipeline internal").setStartingOffsets(OffsetsInitializer.latest())
                    .setDeserializer(new SwitchTupleDeserializer()).build();

            DataStream<Tuple2<Boolean, UUID>> switchingSignal = env.fromSource(switchingReadySignalInput, WatermarkStrategy.noWatermarks(), "switching signal");

            DataStream<Tuple2<String, VehicleContext>> bufferedStream = inputStream.connect(switchingSignal).process(new ControlledBuffer());
            DataStream<Tuple2<String, VehicleContext>> assignedWithKey = bufferedStream.flatMap(new KeyAssigner(parallelism));

            SingleOutputStreamOperator<Tuple2<String, VehicleContext>> processedStream = assignedWithKey.keyBy(new SelectParallelKey())
                    .connect(broadcastStream).process(pet).setParallelism(parallelism);

            DataStream<Tuple3<Integer, Boolean, SwitchingDecision>> feedbackStream = processedStream.getSideOutput(pet.getSignalSideOutput());
            // Due to restriction of Flink, we cannot send the ready signal from the PET Enforcement Operator directly to the message broker.
            // The SideOutput should be collected and then sent to the message broker.
            DataStream<Tuple2<Boolean, UUID>> switchingReadySignalStream = feedbackStream.process(new FeedbackProcessor(parallelism, measure));

            switchingReadySignalStream.sinkTo(getKafkaSink(kafkaTopic));

            return processedStream.process(new DataOrderMaintainer(parallelism, dataSection));
        } else if (parallelism == 1) {
            StatefulSerialPETEnforcementUpgrade<T> pet = new StatefulSerialPETEnforcementUpgrade<>(dataSection, dataType, measure);
            return inputStream.connect(broadcastStream).process(pet);
        } else throw new IllegalArgumentException("Parallelism should be greater or equal to 1.");

    }

    /**
     * Returns a Kafka sink to write the ready signal of the parallel instances of {@link StatelessParallelPETEnforcement} operators.
     *
     * @param kafkaTopic the name of the topic
     * @return a Kafka sink
     */
    private static KafkaSink<Tuple2<Boolean, UUID>> getKafkaSink(String kafkaTopic) {
        Properties properties = new Properties();
        properties.setProperty("transaction.timeout.ms", "360000"); // 1h
        KafkaRecordSerializationSchema<Tuple2<Boolean, UUID>> schema = KafkaRecordSerializationSchema.builder().setTopic(kafkaTopic)
                .setKafkaValueSerializer(SwitchTupleSerializer.class).build();
        return KafkaSink.<Tuple2<Boolean, UUID>>builder().setBootstrapServers(GlobalConfig.BOOTSTRAP_SERVER)
                .setKafkaProducerConfig(properties).setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(kafkaTopic)
                .setRecordSerializer(schema).build();
    }

    /**
     * Select the data elements according the previously assigned key to deterministically dispatch the records to
     * the parallel instances of {@link StatelessParallelPETEnforcement} operator.
     */
    private static class SelectParallelKey implements KeySelector<Tuple2<String, VehicleContext>, Integer> {
        @Override
        public Integer getKey(Tuple2<String, VehicleContext> value) {
            return value.f1.getKey();
        }
    }
}

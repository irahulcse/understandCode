package thesis.util;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import thesis.common.GlobalConfig;
import thesis.context.VehicleContext;
import thesis.flink.Descriptors;
import thesis.flink.SwitchingDecision;
import thesis.jobs.ImageOnly;
import thesis.policy.Policy;
import thesis.policy.SingleDataSectionPolicy;
import thesis.policy.SingleDataSectionSinglePriorityPolicy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * This operator simulates the evaluation of the predefined privacy policies.
 * It is used to generate arbitrary many switching decisions. The switching decision switch between the two PET description as string.
 */
public class SituationEvaluatorSimulator extends KeyedBroadcastProcessFunction<String,
        Tuple2<String, VehicleContext>, Policy, SwitchingDecision> {

    public static OutputTag<Tuple2<String, VehicleContext>> contextOutputTag = new OutputTag<>("context fork", new TypeHint<Tuple2<String, VehicleContext>>() {
    }.getTypeInfo());
    private transient ValueState<Integer> highestSatisfiedPriority;
    private transient ValueState<Long> scalarTimestampState;
    private transient ValueState<Long> locationTimestampState;
    private transient ValueState<SingleDataSectionPolicy> currentPolicy;
    private transient ValueState<SingleDataSectionSinglePriorityPolicy> currentSatisfiedPolicy;

    private final String jsonPath = GlobalConfig.textInputSource + "/petDescriptions/";

    private final String pet1, pet2;
    private String current;
    private static int counter = 1;
    private final Map<Integer, UUID> uuidMap;


    @Override
    public void open(Configuration parameters) throws Exception {
        highestSatisfiedPriority = getRuntimeContext().getState(Descriptors.highestSatisfiedPriority);
        ValueStateDescriptor<Long> scalarTimestampStateDescriptor = new ValueStateDescriptor<>("scalar record time", BasicTypeInfo.LONG_TYPE_INFO);
        ValueStateDescriptor<Long> locationTimestampStateDescriptor = new ValueStateDescriptor<>("location record time", BasicTypeInfo.LONG_TYPE_INFO);
        scalarTimestampState = getRuntimeContext().getState(scalarTimestampStateDescriptor);
        locationTimestampState = getRuntimeContext().getState(locationTimestampStateDescriptor);
        currentPolicy = getRuntimeContext().getState(Descriptors.singleDataSectionPolicyStateDescriptor);
        currentSatisfiedPolicy = getRuntimeContext().getState(Descriptors.atomicPolicyDescriptor);

    }

    public SituationEvaluatorSimulator(Map<Integer, UUID> uuidMap) throws IOException {
        this.uuidMap = uuidMap;
        pet1 = new String(Files.readAllBytes(Paths.get(jsonPath + "imagePET1.json")));
        pet2 = new String(Files.readAllBytes(Paths.get(jsonPath + "imagePET2.json")));
        current = pet1;
    }

    @Override
    public void processElement(Tuple2<String, VehicleContext> value, KeyedBroadcastProcessFunction<String, Tuple2<String, VehicleContext>, Policy, SwitchingDecision>.ReadOnlyContext ctx, Collector<SwitchingDecision> out) throws Exception {
        String key = ctx.getCurrentKey();
        VehicleContext currentContext = value.f1;
        if (Integer.parseInt(value.f1.getImageData().getSequenceNumber()) % (counter * 50) == 0) {
            out.collect(new SwitchingDecision(Collections.singleton("image"), current, System.currentTimeMillis(), uuidMap.get(counter - 1)));
            System.out.println("Emitted switching decision" + uuidMap.get(counter - 1));
            currentContext.setTriggers(Collections.singleton("image"), uuidMap.get(counter - 1));
            if (current.equals(pet1)) {
                current = pet2;
            } else {
                current = pet1;
            }
            if (Integer.parseInt(value.f1.getImageData().getSequenceNumber()) != 0) {
                counter++;
            }
        }
        currentContext.setEvaluationTime(System.currentTimeMillis());
        ctx.output(contextOutputTag, Tuple2.of(key, currentContext));

    }

    @Override
    public void processBroadcastElement(Policy value, KeyedBroadcastProcessFunction<String, Tuple2<String, VehicleContext>, Policy, SwitchingDecision>.Context ctx, Collector<SwitchingDecision> out) throws Exception {
        BroadcastState<String, SingleDataSectionPolicy> broadcastState = ctx.getBroadcastState(Descriptors.policyStateDescriptor);
        broadcastState.clear();
        Set<String> dataSections = value.getAllDataSections();
        for (String dataSection : dataSections) {
            broadcastState.put(dataSection, value.getDataSectionPolicies(dataSection));
            //System.out.println(getRuntimeContext().getTaskNameWithSubtasks() + " updated broadcast state: " + dataSection);
        }
    }

    private void initializeStates(String key) throws IOException {
        if (highestSatisfiedPriority.value() == null) {
            //System.out.println("SituationEvaluator: "+getRuntimeContext().getTaskNameWithSubtasks() + " Key: " + key + ": initialize priority register with default values");
            highestSatisfiedPriority.update(Integer.MAX_VALUE);
        }
        if (scalarTimestampState.value() == null) {
            scalarTimestampState.update(0L);
        }
        if (locationTimestampState.value() == null) {
            locationTimestampState.update(0L);
        }
    }

    private void handleLastPolicy() {
    }
}




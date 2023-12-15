package thesis.util;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import thesis.context.VehicleContext;
import thesis.flink.Descriptors;
import thesis.flink.SwitchingDecision;
import thesis.policy.Policy;
import thesis.policy.SingleDataSectionPolicy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * This operator simulates the evaluation of the predefined privacy policies.
 * It is used to generate arbitrary many switching decisions. The switching decision switch between the two PET description as string.
 */
public class SituationEvaluatorSimulatorUpgrade extends KeyedBroadcastProcessFunction<String,
        Tuple2<String, VehicleContext>, Policy, SwitchingDecision> {

    public static OutputTag<Tuple2<String, VehicleContext>> contextOutputTag = new OutputTag<>("context fork", new TypeHint<Tuple2<String, VehicleContext>>() {
    }.getTypeInfo());

    private final List<String> petPathList;
    private int currentIndex;
    private String currentPETDescription;
    private static int counter = 1;
    private final Map<Integer, UUID> uuidMap;

    public SituationEvaluatorSimulatorUpgrade(Map<Integer, UUID> uuidMap, String... petPathStrings) throws IOException {
        this.uuidMap = uuidMap;
        petPathList = Arrays.asList(petPathStrings);
        currentIndex = 0;
        currentPETDescription = new String(Files.readAllBytes(Paths.get(petPathList.get(currentIndex))));
    }

    @Override
    public void processElement(Tuple2<String, VehicleContext> value, KeyedBroadcastProcessFunction<String, Tuple2<String, VehicleContext>, Policy, SwitchingDecision>.ReadOnlyContext ctx, Collector<SwitchingDecision> out) throws Exception {
        String key = ctx.getCurrentKey();
        VehicleContext currentContext = value.f1;
        if (Integer.parseInt(value.f1.getImageData().getSequenceNumber()) == 0 || Integer.parseInt(value.f1.getImageData().getSequenceNumber()) % (counter * 50) == 0) {
            if (Integer.parseInt(value.f1.getImageData().getSequenceNumber()) == 0){
                currentContext.setEvaluationTime(System.currentTimeMillis());
                ctx.output(contextOutputTag, Tuple2.of(key, currentContext));
                return;
            }
            out.collect(new SwitchingDecision(Collections.singleton("image"), currentPETDescription, System.currentTimeMillis(), uuidMap.get(counter - 1)));
            System.out.println("Emitted switching decision " + uuidMap.get(counter - 1));
            currentContext.setTriggers(Collections.singleton("image"), uuidMap.get(counter - 1));
            if (currentIndex == petPathList.size() - 1) {
                currentIndex = 0;
            } else {
                currentIndex++;
            }
            currentPETDescription = new String(Files.readAllBytes(Paths.get(petPathList.get(currentIndex))));
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

}




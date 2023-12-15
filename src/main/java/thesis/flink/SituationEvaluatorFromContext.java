package thesis.flink;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import thesis.context.VehicleContext;
import thesis.context.predicates.ComplexPredicate;
import thesis.pet.PETDescriptor;
import thesis.policy.Policy;
import thesis.policy.SingleDataSectionPolicy;
import thesis.policy.SingleDataSectionSinglePriorityPolicy;

import java.io.IOException;
import java.util.*;

/**
 * This operator ist keyed by the name of the data section.
 * It performs the evaluation of the policy of the corresponding data section.
 * It takes an instance of {@link VehicleContext} as the input of normal elements and a broadcast input of {@link Policy}.
 * A {@link SwitchingDecision} is emitted if the processed {@link VehicleContext} satisfies a different condition from the last one.
 */
public class SituationEvaluatorFromContext extends KeyedBroadcastProcessFunction<String,
        Tuple2<String, VehicleContext>, Policy, SwitchingDecision> {

    public static OutputTag<Tuple2<String, VehicleContext>> contextOutputTag = new OutputTag<>("context fork", new TypeHint<Tuple2<String, VehicleContext>>() {
    }.getTypeInfo());
    private transient ValueState<Integer> highestSatisfiedPriority;
    private transient ValueState<Long> scalarTimestampState;
    private transient ValueState<Long> locationTimestampState;
    private transient ValueState<SingleDataSectionPolicy> currentPolicy;
    private transient ValueState<SingleDataSectionSinglePriorityPolicy> currentSatisfiedPolicy;

    // private Policy lastPolicyFromDB;
    private final String involvedAppName;


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

    public SituationEvaluatorFromContext(String involvedAppName) {
        this.involvedAppName = involvedAppName;
    }

    @Override
    public void processElement(Tuple2<String, VehicleContext> value, KeyedBroadcastProcessFunction<String, Tuple2<String, VehicleContext>, Policy, SwitchingDecision>.ReadOnlyContext ctx, Collector<SwitchingDecision> out) throws Exception {
        String key = ctx.getCurrentKey();
        //System.out.println(getRuntimeContext().getTaskNameWithSubtasks() + " Key: " + key);

        ReadOnlyBroadcastState<String, SingleDataSectionPolicy> readOnlyBroadcastState = ctx.getBroadcastState(Descriptors.policyStateDescriptor);
        currentPolicy.update(readOnlyBroadcastState.get(key));

        VehicleContext currentContext = value.f1;
        initializeStates();

        if (currentPolicy.value() == null) {
            //System.out.println(getRuntimeContext().getTaskNameWithSubtasks() + " Policy for the key " + key + " is not defined.");
            return;
        }
        PriorityQueue<SingleDataSectionSinglePriorityPolicy> pq = currentPolicy.value().pq;
        for (SingleDataSectionSinglePriorityPolicy sp : pq) {
            ComplexPredicate cp = new ComplexPredicate(sp.predicateAsString);
            if (cp.evaluate(currentContext)) {
                PETDescriptor petDescriptor = new PETDescriptor(sp.petDescription);
                List<String> affectedDataSections = petDescriptor.getOutputDataSections();
                if (affectedDataSections.isEmpty()){
                    // Deal with default noPET concerning all data sections
                    affectedDataSections.add(key);
                }
                switch (highestSatisfiedPriority.value().compareTo(sp.priority)) {
                    // Higher priority means numerically less
                    case 1, -1 -> {
                        // case 1: A predicate of higher priority is satisfied
                        // case -1: The current true predicate is no longer satisfied. A predicate of lower priority is satisfied
                        // In both cases the highest satisfied priority should be updated.
                        UUID uuid = UUID.randomUUID();
                        SwitchingDecision sd = new SwitchingDecision(Collections.singleton(key), sp.petDescription, System.currentTimeMillis(), uuid);
                        currentContext.setTriggers(affectedDataSections, uuid);
                        out.collect(sd);
                        System.out.println("SituationEvaluator: " + key + " Made decision: \n" + sd);
                        if (highestSatisfiedPriority.value().compareTo(sp.priority) > 0){
                            System.out.println("Reason: higher priority satisfied");
                        }else{
                            System.out.println("Reason: higher priority not satisfied anymore. Lower priority satisfied.");
                        }
                        highestSatisfiedPriority.update(sp.priority);
                    }
                    case 0 -> {
                        // Same priority. Check if it is the one already satisfied, since there could be multiple atomic policy with the same priority.
                        // No need to check nonnull, because the comparison must not be zero if there is no previously satisfied predicate.
                        if (!sp.predicateAsString.equals(currentSatisfiedPolicy.value().predicateAsString)) {
                            UUID uuid = UUID.randomUUID();
                            SwitchingDecision sd = new SwitchingDecision(Collections.singleton(key), sp.petDescription, System.currentTimeMillis(), uuid);
                            currentContext.setTriggers(affectedDataSections, uuid);
                            out.collect(sd);
                            System.out.println("SituationEvaluator: " + key + " Made decision: \n" + sd+ "\nReason: Another predicate of the same priority is true");
                        }
                    }
                }
                currentSatisfiedPolicy.update(sp);
            }
        }
        currentContext.setEvaluationTime(System.currentTimeMillis());
        ctx.output(contextOutputTag, Tuple2.of(key, currentContext));
    }

    @Override
    public void processBroadcastElement(Policy value, KeyedBroadcastProcessFunction<String, Tuple2<String, VehicleContext>, Policy, SwitchingDecision>.Context ctx, Collector<SwitchingDecision> out) throws Exception {
        if (!value.getAppName().equals(involvedAppName)) return;
        BroadcastState<String, SingleDataSectionPolicy> broadcastState = ctx.getBroadcastState(Descriptors.policyStateDescriptor);
        broadcastState.clear();
        Set<String> dataSections = value.getAllDataSections();
        for (String dataSection : dataSections) {
            broadcastState.put(dataSection, value.getDataSectionPolicies(dataSection));
            //System.out.println(getRuntimeContext().getTaskNameWithSubtasks() + " updated broadcast state: " + dataSection);
        }
    }

    private void initializeStates() throws IOException {
        if (highestSatisfiedPriority.value() == null) {
            highestSatisfiedPriority.update(Integer.MAX_VALUE);
        }
        if (scalarTimestampState.value() == null) {
            scalarTimestampState.update(0L);
        }
        if (locationTimestampState.value() == null) {
            locationTimestampState.update(0L);
        }
    }

}




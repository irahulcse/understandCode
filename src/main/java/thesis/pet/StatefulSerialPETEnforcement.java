package thesis.pet;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import thesis.common.Colors;
import thesis.common.GlobalConfig;
import thesis.context.VehicleContext;
import thesis.context.data.Data;
import thesis.flink.Descriptors;
import thesis.flink.SwitchingDecision;
import thesis.pet.repo.NoPET;
import thesis.util.DBWrapper;
import thesis.util.OutputTool;

import java.io.IOException;
import java.util.*;

/**
 * An operator that processes the data section of type {@code <T> }in one instance of {@link VehicleContext}.
 * It is used in the centralized PET Enforcement schema.
 * It adjusts the PET algorithm in use according to the {@link SwitchingDecision}, which is received by broadcast.
 * Upon receiving a {@link SwitchingDecision}, the {@link BroadcastState} is updated.
 * The behavior of this operator is divided into four cases:
 * <ul>
 *     <li>Processing a triggering data with processed {@link SwitchingDecision} (is called known broadcast in the following) </li>
 *     <li>Processing a non-triggering data with known broadcast</li>
 *     <li>Processing a triggering data without a known broadcast. This case happens if the {@link SwitchingDecision} arrives late.</li>
 *     <li>Processing a non-triggering data without a known broadcast. This is either a normal operation or the {@link SwitchingDecision}
 *     is even later than the non-triggering data following the corresponding triggering data.</li>
 * </ul>
 * This operator uses a cache of information from the last successful processed {@link SwitchingDecision} to determine
 * if a new broadcast is processed. It compares the {@link BroadcastState} with the cached state.
 * @param <T> The type of the data record of the data section that the residing PET deals with.
 *            If a PET outputs processed data of more than two data sections, then there will be the same number of operators of this kind,
 *            each is responsible for one of the data section.
 */
public class StatefulSerialPETEnforcement<T extends Data<?>> extends BroadcastProcessFunction<Tuple2<String, VehicleContext>, SwitchingDecision, Tuple2<String, VehicleContext>> {

    PETFragment petFragmentWorking;
    PETFragment petFragmentPending;
    private final Queue<VehicleContext> bufferedVehicleContexts = new ArrayDeque<>();
    private Tuple3<String, Long, UUID> currentSwitchingInfo; // Target data section, timestamp, windows size, switching ID
    private boolean isBuffering = false;

    private final String targetDataSection;
    private final Class<T> pETAffectedClass;
    private PETDescriptor petDescription;
    private SwitchingDecision decision;
    private long latestRecordTimestamp = 0L;

    @Override
    public void open(Configuration parameters) throws Exception {
        petFragmentWorking = new NoPET();
        currentSwitchingInfo = Tuple3.of(null, System.currentTimeMillis(), null);

    }

    public StatefulSerialPETEnforcement(String target, Class<T> inputClass) {
        this.targetDataSection = target;
        this.pETAffectedClass = inputClass;
    }

    @Override
    public void processElement(Tuple2<String, VehicleContext> value, BroadcastProcessFunction<Tuple2<String, VehicleContext>, SwitchingDecision, Tuple2<String, VehicleContext>>.ReadOnlyContext ctx, Collector<Tuple2<String, VehicleContext>> out) throws Exception {
        if (value.f1.extractRecord(pETAffectedClass).getGenerateTime()==latestRecordTimestamp){
            //System.out.println(targetDataSection+" Already processed. Discard.");
            return;
        }
        VehicleContext currentVC = value.f1;
        T data = currentVC.extractRecord(pETAffectedClass);
        data.setProcessBegin(System.currentTimeMillis());
//        System.out.println(Colors.ANSI_GREEN + "Current data: " + data + Colors.ANSI_RESET);
//        DebuggingUtils.judge(currentVC.isTrigger());

        ReadOnlyBroadcastState<String, Tuple3<String, Long, UUID>> readOnlyBroadcastState = ctx.getBroadcastState(Descriptors.decisionMapState);
        Tuple3<String, Long, UUID> broadcastSwitchingInfo = readOnlyBroadcastState.get(targetDataSection);

        if (broadcastSwitchingInfo == null) {
            // At the very beginning, the first element arrives earlier than the broadcast switching info.
            isBuffering = true;
            //System.out.println(Colors.ANSI_PURPLE + "Buffer is turned on." + Colors.ANSI_RESET);
            bufferedVehicleContexts.add(currentVC);
            return;
        } else {
            // Buffered records before the first broadcast arrives
            while (!bufferedVehicleContexts.isEmpty()) {
                VehicleContext bufferedVC = bufferedVehicleContexts.poll();
                processVehicleContext(broadcastSwitchingInfo, bufferedVC, out);
            }
        }
        processVehicleContext(broadcastSwitchingInfo, currentVC, out);
    }

    @Override
    public void processBroadcastElement(SwitchingDecision value, BroadcastProcessFunction<Tuple2<String, VehicleContext>, SwitchingDecision, Tuple2<String, VehicleContext>>.Context ctx, Collector<Tuple2<String, VehicleContext>> out) throws Exception {
        if (!value.dataSections.contains(targetDataSection)) return;
        //System.out.println("Responsible for: "+ targetDataSection+ "Received switching decision " + value.decisionID.toString().split("-")[0]);
        decision = value;
        decision.setInstantiationBegin(System.currentTimeMillis());
        BroadcastState<String, Tuple3<String, Long, UUID>> broadcastState = ctx.getBroadcastState(Descriptors.decisionMapState);
        petDescription = new PETDescriptor(value.petDescriptionString);

        for (String ds : value.dataSections) {
            // For multiple output PET, each data section has the same value in the broadcast state.
            broadcastState.put(ds, Tuple3.of(value.petDescriptionString, value.generationTime, value.decisionID));
        }
        if (value.dataSections.contains(targetDataSection)) {
            petFragmentPending = PETProvider.build(petDescription);
            //System.out.println(getRuntimeContext().getTaskNameWithSubtasks() + "For data section: " + value.dataSection + " Next PET: " + petDescription.getName());
            decision.setInstantiationEnd(System.currentTimeMillis());
        }
    }

    private void processVehicleContext(Tuple3<String, Long, UUID> broadcastSwitchingInfo, VehicleContext vc, Collector<Tuple2<String, VehicleContext>> out) {
        if (!broadcastSwitchingInfo.equals(currentSwitchingInfo)) {
            // Clear the buffered data first. The data is buffered due to late broadcast switching info.
            while (!bufferedVehicleContexts.isEmpty()) {
                VehicleContext bufferedVC = bufferedVehicleContexts.poll();
                handleVehicleContextWithKnownBroadcast(broadcastSwitchingInfo, bufferedVC, out);
            }
            // New switching info is already processed.
            handleVehicleContextWithKnownBroadcast(broadcastSwitchingInfo, vc, out);
        } else {
            // Up to this point no new switching info is known. -> No pending PET, no need to warm up.
            // Possible situations: either there's no switch happened, or the incoming record arrives earlier than the switching info.
            if (vc.isTrigger()) {
                handleTriggerWithoutKnownBroadcast(vc, out);
            } else {
                // A normal record
                handleNormalDataWithoutKnownBroadcast(broadcastSwitchingInfo, vc, out);
            }
        }
    }

    private void handleVehicleContextWithKnownBroadcast(Tuple3<String, Long, UUID> broadcastSwitchingInfo, VehicleContext vc, Collector<Tuple2<String, VehicleContext>> out) {
        if (vc.isTrigger()) {
            if (!vc.getTriggerIDs().contains(broadcastSwitchingInfo.f2)) {
                // The trigger corresponding to the broadcast state is still in queue. This trigger doesn't apply to the PET in the broadcast state.
                out.collect(Tuple2.of(targetDataSection, process(vc)));
            } else {
                // This is the trigger for the next switch. Perform the switching process and update the current applicable switching info.
                handleTriggerWithKnownBroadcast(broadcastSwitchingInfo, vc, out);
            }
        } else {
            handleNormalDataWithKnownBroadcast(broadcastSwitchingInfo, vc, out);
        }
    }

    private void handleTriggerWithKnownBroadcast(Tuple3<String, Long, UUID> broadcastSwitchingInfo, VehicleContext vc, Collector<Tuple2<String, VehicleContext>> out) {
        switchPET(vc);
        currentSwitchingInfo = broadcastSwitchingInfo;
        isBuffering = false;
        //System.out.println(Colors.ANSI_PURPLE + "Buffer is turned off." + Colors.ANSI_RESET);
        // Release the buffered elements to warm up the next PET.
        // If the records handed over are not enough to warm up the next PET, continue to warm up using the current element.
        while (!bufferedVehicleContexts.isEmpty()) {
            VehicleContext bufferedContext = bufferedVehicleContexts.poll();
            //T bufferedData = bufferedContext.extractRecord(pETAffectedClass);
            if (!petFragmentWorking.isReady()) {
                petFragmentWorking.buildState(bufferedContext);
            } else {
                // States fully built. Start processing using the next PET.
/*                if (stateBuildingOnGoing) {
                    outputSwitchLatency();
                }*/
                out.collect(Tuple2.of(targetDataSection, process(bufferedContext)));
            }
        }
        //T data = vc.extractRecord(pETAffectedClass);
        if (!petFragmentWorking.isReady()) {
            // If the buffered elements are not enough to warm up the next PET, continue to warm up using the current element.
            petFragmentWorking.buildState(vc);
            //warmUpCountdown--;
            //System.out.println("StatefulSerialPETEnforcement: warming up the pending PET with " + data );
        } else {
/*            if (stateBuildingOnGoing) {
                outputSwitchLatency();
            }*/
            out.collect(Tuple2.of(targetDataSection, process(vc)));
        }
    }

    private void handleTriggerWithoutKnownBroadcast(VehicleContext vc, Collector<Tuple2<String, VehicleContext>> out) {
        if (vc.isAffected(targetDataSection)) {
            // This record arrives earlier and should be applied to the next PET. Buffer it.
            isBuffering = true;
            //System.out.println(Colors.ANSI_PURPLE + "Buffer is turned on." + Colors.ANSI_RESET);
            bufferedVehicleContexts.add(vc);
            //System.out.println("The eigen data section is affected. Buffered: " + vc);
        } else {
            if (isBuffering) {
                // This record is a trigger for other data section. Buffer it if the buffer mode is on.
                bufferedVehicleContexts.add(vc);
                //System.out.println("Buffered context: " + vc);
            } else {
                out.collect(Tuple2.of(targetDataSection, process(vc)));
            }
        }
    }

    private void handleNormalDataWithoutKnownBroadcast(Tuple3<String, Long, UUID> broadcastSwitchingInfo, VehicleContext vc, Collector<Tuple2<String, VehicleContext>> out) {
        //T data = vc.extractRecord(pETAffectedClass);
        if (isBuffering) {
            bufferedVehicleContexts.add(vc);
        } else {
            if (!petFragmentWorking.isReady()) {
                // If the next PET needs a state, then build it with the current element in advance to reduce the waiting time.
                petFragmentWorking.buildState(vc);
                //System.out.println("StatefulSerialPETEnforcement: warming up the pending PET with " + data );
            } else {
/*                if (stateBuildingOnGoing) {
                    outputSwitchLatency();
                }*/
                out.collect(Tuple2.of(targetDataSection, process(vc)));
            }
        }
    }

    private void handleNormalDataWithKnownBroadcast(Tuple3<String, Long, UUID> broadcastSwitchingInfo, VehicleContext vc, Collector<Tuple2<String, VehicleContext>> out) {
        T data = vc.extractRecord(pETAffectedClass);
        if (data.getGenerateTime() < currentSwitchingInfo.f1) {
            // This is an older record waiting to be processed. Comparing with currentSwitchingInfo, because it is definitely updated, since the
            // new switching info is already processed.
            if (!petFragmentWorking.isReady()) {
                // If the next PET needs a state, then build it with the current element in advance to reduce the waiting time.
                petFragmentPending.buildState(vc);
                //System.out.println("StatefulSerialPETEnforcement: warming up the pending PET with " + data );
            }
            out.collect(Tuple2.of(targetDataSection, process(vc)));
        } else {
            // This case is not possible, since the data record arrives later than the trigger timestamp. In this case the trigger must be
            // already processed and thus currentSwitchingInfo will be equal to the broadcastSwitchingInfo.
            System.out.println("StatefulPETEnforcement: " + Colors.ANSI_RED + "value.getGenerateTime() >= currentSwitchingInfo.f1 happened." + Colors.ANSI_RESET);
        }
    }

    /**
     * Perform PET enforcement and set the end of processing in the corresponding data section for further evaluation.
     *
     * @param vc An instance of {@link VehicleContext} to be processed
     * @return A processed instance of {@link VehicleContext}
     */
    private VehicleContext process(VehicleContext vc) {
        VehicleContext processed = petFragmentWorking.execute(vc);
        T data = processed.extractRecord(pETAffectedClass);
        data.setProcessEnd(System.currentTimeMillis());
        processed.update(data);
        latestRecordTimestamp = vc.extractRecord(pETAffectedClass).getGenerateTime();
        return processed;
    }

    /**
     * Switching contains following steps:
     * <ul>
     *     <li>Assign the pending PET to the active PET</li>
     *     <li>Fetch data records in DB</li>
     *     <li>Warm up the new active PET with the records</li>
     * </ul>
     * The input argument serves as the search baseline in the stored history data.
     */
    private void switchPET(VehicleContext vc) {
        decision.setSwitchBegin(System.currentTimeMillis());
        //System.out.println(Colors.ANSI_CYAN + getRuntimeContext().getTaskNameWithSubtasks() + "Switching process begins." + Colors.ANSI_RESET);
        // Step 1
        //System.out.println("Set pending PET to active...");
        petFragmentWorking = petFragmentPending;
        petFragmentPending = null;
        // Step 2 + 3
        List<PETDescriptor.StateWindow> stateWindowList = petDescription.getStateWindow();

        if (petDescription.isStateful()) {
            //System.out.println("Query in DB to prepare for warming up...");
            //long queryStart = System.currentTimeMillis();
            for (PETDescriptor.StateWindow sw : stateWindowList) {
                if (!sw.isStateful()) continue;
                Class<? extends Data<?>> recordClass = GlobalConfig.sectionRecordClassMap.get(sw.getDataSection());
                List<? extends Data<?>> fromDB = DBWrapper.prepareWarmUp(sw, recordClass, vc).getValue();

                Map.Entry<String, List<? extends Data<?>>> handOver = Map.entry(sw.getDataSection(), fromDB);
                petFragmentWorking.buildState(handOver);
                System.out.println(sw.getDataSection() + " is warmed up.");
            }
            //System.out.println("Database Access Latency: "+ (System.currentTimeMillis()-queryStart));
            //outputSwitchLatency();
        }
        // Record measurement
        decision.setSwitchEnd(System.currentTimeMillis());
        decision.setEvaluationMode(SwitchingDecision.CENTRALIZED);
        try {
            OutputTool.log(decision, "");
        } catch (IOException e) {
            System.out.println(Colors.ANSI_RED + "failed to output SwitchingDecision" + Colors.ANSI_RESET);
        }

        System.out.println(Colors.ANSI_CYAN + " switching finished." + Colors.ANSI_RESET);
    }
}

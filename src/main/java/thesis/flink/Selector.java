package thesis.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import thesis.context.VehicleContext;
import thesis.context.data.Data;
import thesis.pet.PETDescriptor;
import thesis.util.OutputTool;

import java.io.Serializable;
import java.util.*;

/**
 * <p>The selector is used only in the baseline of evaluation: output filtering.</p>
 * The selector filters the incoming processed {@link VehicleContext} according to the current applicable PETs.
 * The upstream of this selector is {@link SimpleStatefulSerialPETEnforcement}, which emits a {@link Tuple2}
 * containing the applied PET (at {@link Tuple2#f0}) and the processed {@link VehicleContext} (at {@link Tuple2#f1})
 * by this PET.
 *
 * @param <T> the type of the data section for which this operator is responsible.
 */
public class Selector<T extends Data<?>> extends CoProcessFunction<Tuple2<String, VehicleContext>, SwitchingDecision, T> {

    private final String myDataSection;
    private final Class<T> pETAffectedClass;
    private Tuple2<String, Long> lastSwitchingInfo;
    private Tuple2<String, Long> currentSwitchingInfo;
    private Tuple2<String, Long> nextSwitchingInfo;

    private final PriorityQueue<Tuple2<String, VehicleContext>> queue = new PriorityQueue<>(new RecordsAscendingTimestamp());
    private boolean isBuffering = false;
    private boolean switched = false;
    private final int numberOfSources;
    private int triggerCount = 0;

    public Selector(String dataSection, int number, Class<T> inputClass) {
        this.myDataSection = dataSection;
        this.pETAffectedClass = inputClass;
        this.numberOfSources = number;
    }

    @Override
    public void processElement1(Tuple2<String, VehicleContext> value, CoProcessFunction<Tuple2<String, VehicleContext>, SwitchingDecision, T>.Context ctx, Collector<T> out) {
        VehicleContext vc = value.f1;
        String petLabel = value.f0;
        if (isBuffering) {
            if (vc.isTrigger()) {
                triggerCount++;
                System.out.println("Increased trigger count to " + triggerCount);
            }
            queue.add(value);
            System.out.println(value.f1.getScalarData() + " is buffered due to buffering mode.");
            return;
        }

        if (vc.isTrigger()) {
            triggerCount++;
            System.out.println("Increased trigger count to " + triggerCount);
            if (nextSwitchingInfo == null) {
                if (!switched) {
                    isBuffering = true;
                    queue.add(value);
                    System.out.println(value.f1.scalarData + " is buffered due to transition mode without known switching event.");
                }
            } else if (value.f0.equals(nextSwitchingInfo.f0)) {
                updateSwitchingInfo();
                emitData(vc, out);
            }
            restoreTransitionState();
            return;
        }
//        if (currentSwitchingInfo == null) {
//            // Fresh after initialization
//            queue.add(value);
//            System.out.println(value.f1.getScalarData() + " is buffered due to fresh start.");
//            return;
//        }
        if (nextSwitchingInfo == null && !vc.isTrigger()) {
            if (vc.getUpdateTimestamp() < currentSwitchingInfo.f1 && petLabel.equals(lastSwitchingInfo.f0)) {
                emitData(vc, out);
            } else {
                if (petLabel.equals(currentSwitchingInfo.f0)) {
                    emitData(vc, out);
                }
            }
            return;
        }
        System.out.println("Discarded: " + vc.scalarData);


    }

    @Override
    public void processElement2(SwitchingDecision value, CoProcessFunction<Tuple2<String, VehicleContext>, SwitchingDecision, T>.Context ctx, Collector<T> out) throws Exception {
        if (!value.dataSections.contains(myDataSection)) return; // Nothing to do with me

/*        if (currentSwitchingInfo == null && nextSwitchingInfo == null) {
            // Initialized state. This is the first processed element.
            // currentSwitchingInfo = Tuple2.of(PETDescriptor.getLabel(value.petDescriptionString), value.generationTime);
            nextSwitchingInfo = Tuple2.of(PETDescriptor.getLabel(value.petDescriptionString), value.generationTime);
            System.out.println("Set next switching info to " + nextSwitchingInfo.f0);
            releaseBuffer(out);
            return;
        }*/
        nextSwitchingInfo = Tuple2.of(PETDescriptor.getLabel(value.petDescriptionString), value.generationTime);
        value.setSwitchEnd(System.currentTimeMillis());
        value.setEvaluationMode(SwitchingDecision.BENCHMARK);
        System.out.println("Set next switching info to " + nextSwitchingInfo.f0);
        releaseBuffer(out);
        OutputTool.log(value,"");
    }

    private static class RecordsAscendingTimestamp implements Comparator<Tuple2<String, VehicleContext>>, Serializable {

        @Override
        public int compare(Tuple2<String, VehicleContext> o1, Tuple2<String, VehicleContext> o2) {
            return (int) (o1.f1.getUpdateTimestamp() - o2.f1.getUpdateTimestamp());
        }
    }

    private void updateSwitchingInfo() {
        lastSwitchingInfo = currentSwitchingInfo;
        currentSwitchingInfo = nextSwitchingInfo;
        nextSwitchingInfo = null;
        switched = true;
        System.out.println("Switching info updated.");
    }

    private void emitData(VehicleContext vc, Collector<T> out) {
        T data = vc.extractRecord(pETAffectedClass);
        data.setPresentationTime(System.currentTimeMillis());
        out.collect(data);
    }

    private void releaseBuffer(Collector<T> out) {
        System.out.println("Releasing buffer...");
        isBuffering = false;
        while (!queue.isEmpty()) {
            Tuple2<String, VehicleContext> record = queue.poll();
            VehicleContext vc = record.f1;
            String petLabel = record.f0;
            if (vc.isTrigger()) {
                if (record.f0.equals(nextSwitchingInfo.f0)) {
                    // It is the triggering data processed by the correct PET
                    // Update the switching info and release this data
                    //updateSwitchingInfo();
                    emitData(vc, out);
                }
            } else {
                if (vc.getUpdateTimestamp() < currentSwitchingInfo.f1 && petLabel.equals(currentSwitchingInfo.f0)) {
                    emitData(vc, out);
                } else if (vc.getUpdateTimestamp() < lastSwitchingInfo.f1 && petLabel.equals(lastSwitchingInfo.f0)) {
                    emitData(vc, out);
                } else {
                    System.out.println("Unexpected data: older than the last switching information.");
                }
            }
        }
        updateSwitchingInfo();
        restoreTransitionState();
        System.out.println("Buffer is empty.");
    }

    private void restoreTransitionState(){
        if (switched && triggerCount == numberOfSources) {
            switched = false;
            triggerCount = 0;
            System.out.println("Transition state restored.");
        }
    }
}

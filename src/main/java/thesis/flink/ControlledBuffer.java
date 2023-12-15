package thesis.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import thesis.context.VehicleContext;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.UUID;

public class ControlledBuffer extends CoProcessFunction<Tuple2<String, VehicleContext>, Tuple2<Boolean, UUID>, Tuple2<String, VehicleContext>> {

    private boolean parallelInstancesReady = false;
    private Tuple2<Boolean, UUID> switchingInfo = Tuple2.of(false, null);
    private final Queue<VehicleContext> recordBuffer = new ArrayDeque<>();

    @Override
    public void processElement1(Tuple2<String, VehicleContext> value, CoProcessFunction<Tuple2<String, VehicleContext>, Tuple2<Boolean, UUID>, Tuple2<String, VehicleContext>>.Context ctx, Collector<Tuple2<String, VehicleContext>> out) throws Exception {
        VehicleContext vc = value.f1;

        if (vc.isTrigger() && !vc.getTriggerIDs().contains(switchingInfo.f1)) {
            // The triggering element arrives earlier than the processor ready signal.
            //System.out.println("ControlledBuffer: " + getRuntimeContext().getTaskNameWithSubtasks() + " receives triggering value. Start buffering element with timestamp " + vc.getImageData().getGenerateTime());
            //System.out.println("ControlledBuffer: " + getRuntimeContext().getTaskNameWithSubtasks() + " set ready signal to false");
            recordBuffer.add(vc);
            parallelInstancesReady = false;
            return;
        }
        if (!parallelInstancesReady) {
            //System.out.println("ControlledBuffer: " + getRuntimeContext().getTaskNameWithSubtasks() + " Parallel instances not ready. Buffering with timestamp " + vc.getImageData().getGenerateTime());
            recordBuffer.add(vc);
        } else {
            // System.out.println("ControlledBuffer: " + getRuntimeContext().getTaskNameWithSubtasks() + " Parallel instances is ready.");
            while (!recordBuffer.isEmpty()) {
                VehicleContext bufferedVC = recordBuffer.poll();
                if (bufferedVC.isTrigger() && !bufferedVC.getTriggerIDs().contains(switchingInfo.f1)){
                    parallelInstancesReady = false;
                    return;
                }
                out.collect(Tuple2.of(value.f0,bufferedVC));
                //System.out.println("ControlledBuffer: " + getRuntimeContext().getTaskNameWithSubtasks() + " Emitted buffered elements with timestamp " + vc.getImageData().getGenerateTime());
            }
            //System.out.println("ControlledBuffer: " + getRuntimeContext().getTaskNameWithSubtasks() + " Buffer is empty. Emitting the latest received element with timestamp " + vc.getImageData().getGenerateTime());
            out.collect(Tuple2.of(value.f0,vc));
        }
    }

    @Override
    public void processElement2(Tuple2<Boolean, UUID> value, CoProcessFunction<Tuple2<String, VehicleContext>, Tuple2<Boolean, UUID>, Tuple2<String, VehicleContext>>.Context ctx, Collector<Tuple2<String, VehicleContext>> out) throws Exception {
       // System.out.println("ControlledBuffer: " + getRuntimeContext().getTaskNameWithSubtasks() + " Received from kafka topic at time " + System.currentTimeMillis());

        if (value.f0.equals(Boolean.TRUE)) {
            //System.out.println("ControlledBuffer: " + getRuntimeContext().getTaskNameWithSubtasks() + " set ready signal to true");
            switchingInfo = value;
            parallelInstancesReady = true;
        }
    }

}

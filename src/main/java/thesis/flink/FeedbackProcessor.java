package thesis.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import thesis.jobs.ImageOnly;
import thesis.util.OutputTool;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * A processor that collects the feedback of successful instantiation of a new PET from the side output of the PET enforcement
 * processor. The signals are categorized by the switching UUID. If the signals from all the parallel instances of the PET
 * enforcement have arrived, this processor then emits the signal that the PET enforcement processor is ready.
 */
public class FeedbackProcessor extends ProcessFunction<Tuple3<Integer, Boolean, SwitchingDecision>, Tuple2<Boolean, UUID>> {
    private final int parallelism;
    private final Map<UUID, Integer> registerMap = new HashMap<>();
    // For measuring sync delay
    private UUID lastUUID;
    private boolean measure = false;

    public FeedbackProcessor(int parallelism, boolean measure) {
        this.parallelism = parallelism;
        this.measure = measure;
    }

    @Override
    public void processElement(Tuple3<Integer, Boolean, SwitchingDecision> value, ProcessFunction<Tuple3<Integer, Boolean, SwitchingDecision>, Tuple2<Boolean, UUID>>.Context ctx, Collector<Tuple2<Boolean, UUID>> out) throws Exception {
        //System.out.println("FeedbackProcessor: " + getRuntimeContext().getTaskNameWithSubtasks()+ " Received switching ID: " + value.f2.toString());
        SwitchingDecision decision = value.f2;
        UUID uuid = decision.decisionID;
        // Record the arrival time of the first ready signal
        if (!uuid.equals(lastUUID)){
            lastUUID = uuid;
        }
        if (!registerMap.containsKey(uuid)) {
            registerMap.put(uuid, 1 << value.f0);
            //System.out.println("FeedbackProcessor: " + getRuntimeContext().getTaskNameWithSubtasks()+ " Added " + value.f2.toString() + " and set to "+ (1 << value.f0));
            return;
        }
        int mask = 1 << value.f0;
        int register = registerMap.get(uuid);
        if ((register & mask) == mask) {
            System.out.println("Switching signal " + value.f2.toString() + ": Duplicated positive at position " + value.f0);
        }
        register |= 1 << value.f0;
        registerMap.put(uuid, register);
        //System.out.println("Current register: " + Integer.toBinaryString(register));
        if (register == (1 << parallelism) - 1) {
            decision.setSyncEnd(System.currentTimeMillis());
            out.collect(Tuple2.of(true, uuid));
            if (measure) {
                OutputTool.log(decision, "feedback");
            }
            System.out.println("Feedback Processor: Output true to kafka topic at time " + System.currentTimeMillis());
            registerMap.remove(uuid);
        }
    }
}

package thesis.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import thesis.context.VehicleContext;
import thesis.context.data.Data;
import thesis.context.data.ImageData;

import java.util.*;

/**
 * A class that restores the original order of the data records that are processed by parallel process function instances.
 * The order is sorted based on the timestamps in the data records. This process function itself should be set with parallelism 1.
 */
public class DataOrderMaintainer extends ProcessFunction<Tuple2<String, VehicleContext>, Tuple2<String, VehicleContext>> {

    private final ArrayList<Queue<VehicleContext>> dataFromEachParallelProcessor = new ArrayList<>();
    private final Map<Integer, Integer> keyProcessorIndexMap = new HashMap<>();
    private final int maxParallelism;
    private final int processorParallelism;
    private int register;
    private final int fullRegister;
    private String targetDataSection;

    public DataOrderMaintainer(int parallelism, String dataSection) {
        this.targetDataSection = dataSection;
        maxParallelism = KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism);
        processorParallelism = parallelism;
        register = 0x00;
        fullRegister = (1 << parallelism) - 0x01;
        for (int i = 0; i < parallelism; i++) {
            dataFromEachParallelProcessor.add(new ArrayDeque<>());
        }
    }

    @Override
    public void processElement(Tuple2<String, VehicleContext> value, ProcessFunction<Tuple2<String, VehicleContext>, Tuple2<String, VehicleContext>>.Context ctx, Collector<Tuple2<String, VehicleContext>> out) {
        VehicleContext vc = value.f1;
        int processorIndex = getProcessorIndex(vc.getKey());
/*        if (value instanceof ImageData){
            System.out.println("DataOrderMaintainer: received image of sequence number "+ ((ImageData)value).getSequenceNumber()+ ", which is processed by #" + processorIndex);
        }*/
        dataFromEachParallelProcessor.get(processorIndex).add(vc);
        // Set the corresponding bit in the register to 1
        register |= 1 << processorIndex;
        //System.out.println("DataOrderMaintainer: Current register " + Integer.toBinaryString(register) + " with full register " + Integer.toBinaryString(fullRegister));
        if (register == fullRegister) {
            // has received records from all parallel instances
            for (int i = 0; i < dataFromEachParallelProcessor.size(); i++) {
                Queue<VehicleContext> dataQueue = dataFromEachParallelProcessor.get(i);
/*                if (value instanceof ImageData){
                    System.out.println("DataOrderMaintainer: emitted image of sequence number "+ ((ImageData)dataQueue.peek()).getSequenceNumber());
                }*/
                VehicleContext queuedVC = dataQueue.poll();
                if (queuedVC.getImageData() != null) {
                    queuedVC.getImageData().setPresentationTime(System.currentTimeMillis());
                }
                out.collect(Tuple2.of(targetDataSection, queuedVC));
                // Reset the index of an empty queue to 0
                if (dataQueue.peek() == null) {
                    register &= ~(1 << i);
                }
            }
        }
    }

    private int getProcessorIndex(int key) {
        if (keyProcessorIndexMap.containsKey(key)) {
            return keyProcessorIndexMap.get(key);
        } else {
            int processorIndex = KeyGroupRangeAssignment.assignKeyToParallelOperator(key, maxParallelism, processorParallelism);
            keyProcessorIndexMap.put(key, processorIndex);
            return processorIndex;
        }
    }


}

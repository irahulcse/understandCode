package thesis.flink;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import thesis.context.VehicleContext;
import thesis.context.data.Data;

/**
 * An operator that assembles the raw sensor reading into a vehicle context containing two fields.
 * It is currently deprecated and not maintained.
 * It might be usable if the sensor readings are delivered from external systems and should be assembled before processing.
 * If the streaming job receives assembled vehicle context,
 * this class can be discarded.
 * @param <IN1>
 * @param <IN2>
 */
public class TwoInputContextBuilder<IN1 extends Data<?>, IN2 extends Data<?>> extends KeyedCoProcessFunction<String, IN1, IN2, VehicleContext> {

    private transient ValueState<IN1> input1State;
    private transient ValueState<IN2> input2State;

    @Override
    public void open(Configuration parameters) throws Exception {
        input1State = (ValueState<IN1>) getRuntimeContext().getState(Descriptors.intermediateBufferForContext);
        input2State = (ValueState<IN2>) getRuntimeContext().getState(Descriptors.intermediateBufferForContext);
    }

    @Override
    public void processElement1(IN1 value, KeyedCoProcessFunction<String, IN1, IN2, VehicleContext>.Context ctx, Collector<VehicleContext> out) throws Exception {
        System.out.println("Process Element 1: received "+value);
        IN2 input2 = input2State.value();
        if (input2 != null) {
            VehicleContext vc = new VehicleContext();
            vc.update(value);
            vc.update(input2);
            out.collect(vc);
            System.out.println("Emitted context " + vc );
        } else {
            input1State.update(value);
        }
    }

    @Override
    public void processElement2(IN2 value, KeyedCoProcessFunction<String, IN1, IN2, VehicleContext>.Context ctx, Collector<VehicleContext> out) throws Exception {
        System.out.println("Process Element 2: received "+value);
        IN1 input1 = input1State.value();
        if (input1 != null) {
            VehicleContext vc = new VehicleContext();
            vc.update(value);
            vc.update(input1);
            out.collect(vc);
            //System.out.println(getRuntimeContext().getTaskNameWithSubtasks() +" Form context tuple <" + input1 + ", "+value+">");
            System.out.println("Emitted context " + vc );
        } else {
            input2State.update(value);
        }
    }
}

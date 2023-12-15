package thesis.demo;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import thesis.context.VehicleContext;
import thesis.context.data.ImageData;
import thesis.context.data.LocationData;
import thesis.context.data.ScalarData;
import thesis.flink.Descriptors;

import java.util.Iterator;

/**
 * A hardcoded reconstructor for processed vehicle data. It assembles the processed data to the vehicle context instances
 * exactly as the input vehicle context instances.
 * The hard-coded parameter is the counter.
 * The purpose of hard-code is for demonstration only.
 */
public class VehicleContextReconstructor extends KeyedProcessFunction<String, Tuple2<String, VehicleContext>, Tuple2<String, VehicleContext>> {
    private final String appName;
    private final int counter = 10; // hard-coded
    private transient ValueState<Integer> counterState;
    private transient ListState<LocationData> currentLocationValue;
    private transient ListState<ImageData> currentImageValue;
    private transient ListState<ScalarData> currentScalarValue;

    public VehicleContextReconstructor(String appName) {
        this.appName = appName;
        //this.dataSections = dataSections;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //currentVehicleContext = getRuntimeContext().getState(Descriptors.vehicleContextToPresent);
        currentScalarValue = getRuntimeContext().getListState(Descriptors.scalarValueList);
        currentImageValue = getRuntimeContext().getListState(Descriptors.imageDataList);
        currentLocationValue = getRuntimeContext().getListState(Descriptors.locationValueList);
        counterState = getRuntimeContext().getState(Descriptors.counter);
    }

    @Override
    public void processElement(Tuple2<String, VehicleContext> value, KeyedProcessFunction<String, Tuple2<String, VehicleContext>, Tuple2<String, VehicleContext>>.Context ctx, Collector<Tuple2<String, VehicleContext>> out) throws Exception {
        writeKnowledge(value.f0, value.f1);
        if (isComplete()) {
            while (isComplete() && currentImageValue.get().iterator().hasNext()) {
                VehicleContext output = getRefreshedVC();
                out.collect(Tuple2.of(appName, output));
                if (counterState.value() == null) {
                    counterState.update(1);
                } else {
                    counterState.update(counterState.value() + 1);
                }
                if (counterState.value().equals(counter)) {
                    removeFirst(currentScalarValue);
                    removeFirst(currentLocationValue);
                    counterState.update(0);
                }
            }
        }
    }

    private void writeKnowledge(String channelName, VehicleContext vc) throws Exception {
        switch (channelName) {
            case "speed" -> currentScalarValue.add(vc.extractRecord(ScalarData.class));
            case "location" -> currentLocationValue.add(vc.extractRecord(LocationData.class));
            case "image" -> currentImageValue.add(vc.extractRecord(ImageData.class));
            default ->
                    throw new IllegalArgumentException("Unknown label: " + channelName + ". How to update this value is not defined.");
        }
    }

    private boolean isComplete() throws Exception {
        Iterator<ScalarData> scalarDataIterator = currentScalarValue.get().iterator();
        if (!scalarDataIterator.hasNext()) return false;
        Iterator<LocationData> locationDataIterator = currentLocationValue.get().iterator();
        if (!locationDataIterator.hasNext()) return false;
        Iterator<ImageData> imageDataIterator = currentImageValue.get().iterator();
        return imageDataIterator.hasNext();
    }

    private void removeFirst(ListState<?> listState) throws Exception {
        Iterator<?> iterator = listState.get().iterator();
        iterator.next();
        iterator.remove();
    }

    private VehicleContext getRefreshedVC() throws Exception {
        ScalarData scalarData = currentScalarValue.get().iterator().next();
        LocationData locationData = currentLocationValue.get().iterator().next();
        Iterator<ImageData> imageDataIterator = currentImageValue.get().iterator();
        ImageData imageData = imageDataIterator.next();
        VehicleContext vc = new VehicleContext();
        vc.update(scalarData);
        vc.update(locationData);
        vc.update(imageData);
        imageDataIterator.remove();
        return vc;
    }
}

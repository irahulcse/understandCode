package thesis.pet.repo;

import thesis.context.VehicleContext;
import thesis.context.data.Data;
import thesis.context.data.ScalarData;
import thesis.pet.PETFragment;

import java.util.*;

public class AverageSpeed implements PETFragment {

    private final Deque<ScalarData> dataForState;
    private final int size;

    public AverageSpeed(Integer size) {
        this.dataForState = new ArrayDeque<>();
        this.size = size;
    }

    @Override
    public VehicleContext execute(VehicleContext in) {
        if (dataForState.size() > (size -1)) {
            dataForState.pollFirst();
        }
        ScalarData scalarData = in.getScalarData();
        dataForState.addLast(scalarData);
        double sum = 0;
        for (ScalarData sd : dataForState) {
            sum += sd.getData();
        }
        scalarData.setProcessedData(sum / dataForState.size());
        scalarData.setProcessTime(System.currentTimeMillis());
        in.update(scalarData);
        return in;
    }

    @Override
    public boolean isReady() {
        return dataForState.size() >= (size - 1);
    }

    @Override
    public void buildState(VehicleContext record) {
        ScalarData sd = record.getScalarData();
        dataForState.addLast(sd);
        if (dataForState.size() > (size -1)) {
            dataForState.pollFirst();
        }

    }

    @Override
    public void buildState(Map.Entry<String, List<? extends Data<?>>> records) {
        List<ScalarData> warmUpData = new ArrayList<>();
        if (records.getKey().equals("speed")) {
            for (Data<?> data : records.getValue()) {
                warmUpData.add((ScalarData) data);
            }
        }
        for (ScalarData sd : warmUpData) {
            dataForState.addLast(sd);
            System.out.println("Warmed up with " + sd);
            if (dataForState.size() > (size -1) ) {
                dataForState.pollFirst();
            }
        }
    }

}

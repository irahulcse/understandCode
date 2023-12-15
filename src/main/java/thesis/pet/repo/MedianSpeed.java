package thesis.pet.repo;

import thesis.context.VehicleContext;
import thesis.context.data.Data;
import thesis.context.data.ScalarData;
import thesis.pet.PETFragment;

import java.util.*;

public class MedianSpeed implements PETFragment {
    private final Deque<ScalarData> dataForState;
    private final int size;

    public MedianSpeed(Integer size) {
        this.dataForState = new ArrayDeque<>();
        this.size = size;
    }

    @Override
    public VehicleContext execute(VehicleContext in) {
        ScalarData data = in.getScalarData();
        dataForState.poll();
        dataForState.add(data);
        //System.out.println("Execute the data "+data+ " Current deque: "+originalData);
        int medianIndex = getMedianIndex();
        List<ScalarData> records = new ArrayList<>(dataForState);
        data.setProcessedData(records.get(medianIndex).getData());
        data.setProcessTime(System.currentTimeMillis());
        in.update(data);
        return in;
    }

    @Override
    public void buildState(VehicleContext record) {
        ScalarData data = record.getScalarData();
        if (dataForState.size() >= size) {
            dataForState.poll();
        }
        dataForState.addFirst(data);
        System.out.println("MedianSpeed: warm up with "+ record);
        System.out.println("Current Deque: "+ dataForState);
    }

    @Override
    public void buildState(Map.Entry<String, List<? extends Data<?>>> records) {
        List<ScalarData> warmUpData = new ArrayList<>();
        if (records.getKey().equals("speed")) {
            for (Data<?> data : records.getValue()) {
                warmUpData.add((ScalarData) data);
            }
        }
        if (warmUpData.size() > size){
            PriorityQueue<ScalarData> sorted = new PriorityQueue<>(new RecordsDescendingTimestamp<>());
            sorted.addAll(warmUpData);
            for (int i = 0 ; i< size; i++){
                buildWithRawData(sorted.poll());
            }
        }else{
            for (ScalarData ld : warmUpData){
                buildWithRawData(ld);
            }
        }
    }

    private void buildWithRawData(ScalarData sd){
        if (dataForState.size() >= size) {
            dataForState.poll();
        }
        dataForState.addFirst(sd);
        System.out.println("MedianSpeed: warm up with "+ sd);
        System.out.println("Current Deque: "+ dataForState);
    }

    @Override
    public boolean isReady() {
        return dataForState.size() >= (size - 1);
    }

    private static class RecordsDescendingTimestamp<T extends Data<?>> implements Comparator<T> {
        @Override
        public int compare(T o1, T o2) {
            return (int) (o2.getGenerateTime() - o1.getGenerateTime());
        }
    }

    private static class RecordsAscendingTimestamp<T extends Data<?>> implements Comparator<T> {
        @Override
        public int compare(T o1, T o2) {
            return (int) (o1.getGenerateTime() - o2.getGenerateTime());
        }
    }

    private int getMedianIndex() {
        // Even number: take the left element. Odd number, the middle one.
        return size/2;
    }
}

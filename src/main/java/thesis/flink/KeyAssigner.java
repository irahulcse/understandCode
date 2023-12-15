package thesis.flink;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.util.Collector;
import thesis.context.VehicleContext;
import thesis.context.data.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * A function that computes and assigns a set of keys to the data records,  so that they can be evenly
 * distributed to each parallel instance of the PET algorithm. The key is computed the same way as in Flink, using {@link KeyGroupRangeAssignment}
 */

public class KeyAssigner extends RichFlatMapFunction<Tuple2<String, VehicleContext>, Tuple2<String, VehicleContext>> {

    private List<Integer> keySet;
    private int currentIndex = 0;

    private final int parallelism;

    public KeyAssigner(int parallelism) {
        this.parallelism = parallelism;
    }

    @Override
    public void flatMap(Tuple2<String, VehicleContext> value, Collector<Tuple2<String, VehicleContext>> out) throws Exception {
        value.f1.setKey(keySet.get(currentIndex));
        out.collect(value);
        updateIndex();
    }


    private List<Integer> calculateKeys() {
        List<Integer> integerKeysList = new ArrayList<>();
        int operatorIndex = 0;
        int i = 0;
        int maxParallelism = KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism);
        while (operatorIndex < parallelism) {
            int index = KeyGroupRangeAssignment.assignKeyToParallelOperator(i, maxParallelism, parallelism);
            if (operatorIndex == index) {
                integerKeysList.add(i);
                System.out.println("Key " + i + " will be processed at processor " + index);
                operatorIndex += 1;
            }
            i++;
        }
        return integerKeysList;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        keySet = calculateKeys();
    }

    private void updateIndex() {
        if (currentIndex == parallelism - 1) {
            currentIndex = 0;
        } else {
            currentIndex += 1;
        }
    }
}

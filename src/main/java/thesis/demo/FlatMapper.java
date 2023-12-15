package thesis.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import thesis.context.VehicleContext;
import thesis.context.data.Data;
import thesis.context.data.ScalarData;

public class FlatMapper<T extends Data<?>> implements FlatMapFunction<T, Tuple2<String, VehicleContext>> {

    @Override
    public void flatMap(T value, Collector<Tuple2<String, VehicleContext>> out) throws Exception {
        VehicleContext vc = new VehicleContext();
        vc.update(value);
        out.collect(Tuple2.of(value.dataSection, vc));
    }
}

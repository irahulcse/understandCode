package thesis.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import thesis.context.VehicleContext;
import thesis.context.data.LocationData;
import thesis.context.data.ScalarData;

import java.util.ArrayList;
import java.util.List;

/**
 * Fan out an instance of {@link VehicleContext} with the names of every available data section.
 * This operation equivalently copies the instances and tags them with a label of the data section name.
 * The situation evaluation step in downstream can evaluate the instances with the same content in parallel.
 * Each data-section-specific evaluator picks the corresponding label.
 */
public class ContextFanOutForKeys extends RichFlatMapFunction<VehicleContext, Tuple2<String, VehicleContext>> {

    private List<String> dataSectionList = new ArrayList<>();
    @Override
    public void flatMap(VehicleContext value, Collector<Tuple2<String, VehicleContext>> out) throws Exception {
        for (String section : dataSectionList){
            out.collect(Tuple2.of(section, value));
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        VehicleContext vc = new VehicleContext();
        dataSectionList = vc.getAllDataSections();
    }
}

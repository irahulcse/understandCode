package thesis.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import thesis.context.VehicleContext;

public class FilterByDataSection implements FilterFunction<Tuple2<String, VehicleContext>> {

    private final String dataSection;

    public FilterByDataSection(String dataSection) {
        this.dataSection = dataSection;
    }

    @Override
    public boolean filter(Tuple2<String, VehicleContext> value) throws Exception {
        return value.f0.equalsIgnoreCase(dataSection);
    }
}

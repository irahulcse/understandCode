package thesis.flink.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import thesis.common.GlobalConfig;
import thesis.context.VehicleContext;
import thesis.context.data.Data;
import thesis.util.OutputTool;

public class DataLogger implements SinkFunction<Tuple2<String, VehicleContext>> {

    @Override
    public void invoke(Tuple2<String, VehicleContext> value, Context context) throws Exception {
        Data<?> processedData = value.f1.extractRecord(GlobalConfig.sectionRecordClassMap.get(value.f0));
        OutputTool.log(processedData, "");
    }
}

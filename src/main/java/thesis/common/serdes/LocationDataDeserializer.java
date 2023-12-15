package thesis.common.serdes;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import thesis.context.data.LocationData;

import java.awt.geom.Point2D;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class LocationDataDeserializer implements KafkaRecordDeserializationSchema<LocationData> {

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<LocationData> out) throws IOException {
        String value = new String(record.value(),StandardCharsets.UTF_8);
        String[] parts = value.split(",");
        double[] coords = new double[2];
        coords[0] = Double.parseDouble(parts[0]);
        coords[1] = Double.parseDouble(parts[1]);
        out.collect(new LocationData(new Point2D.Double(coords[0],coords[1])));
    }

    @Override
    public TypeInformation<LocationData> getProducedType() {
        return TypeInformation.of(LocationData.class);
    }
}

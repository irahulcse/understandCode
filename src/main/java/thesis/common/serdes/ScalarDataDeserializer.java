package thesis.common.serdes;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.json.JSONObject;
import thesis.context.data.ScalarData;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public class ScalarDataDeserializer implements KafkaRecordDeserializationSchema<ScalarData> {
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<ScalarData> out) throws IOException {
        String key = new String(record.key(), StandardCharsets.UTF_8);
        String value = new String(record.value(),StandardCharsets.UTF_8);
        Double d = Double.parseDouble(value);
        out.collect(new ScalarData(key, d));
    }

    @Override
    public TypeInformation<ScalarData> getProducedType() {
        return TypeInformation.of(ScalarData.class);
    }
}

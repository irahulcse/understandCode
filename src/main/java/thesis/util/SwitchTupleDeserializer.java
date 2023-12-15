package thesis.util;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class SwitchTupleDeserializer implements KafkaRecordDeserializationSchema<Tuple2<Boolean, UUID>> {
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Tuple2<Boolean, UUID>> out) throws IOException {
        String s = new String(record.value(), StandardCharsets.UTF_8);
        String[] parts = s.split(",");
        out.collect(Tuple2.of(Boolean.valueOf(parts[0]), UUID.fromString(parts[1])));
    }

    @Override
    public TypeInformation<Tuple2<Boolean, UUID>> getProducedType() {
        return new TypeHint<Tuple2<Boolean, UUID>>(){}.getTypeInfo();
    }
}

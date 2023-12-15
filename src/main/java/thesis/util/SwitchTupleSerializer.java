package thesis.util;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.kafka.common.serialization.Serializer;

import java.util.UUID;

public class SwitchTupleSerializer implements Serializer<Tuple2<Boolean, UUID>> {
    @Override
    public byte[] serialize(String topic, Tuple2<Boolean, UUID> data) {
        String s = data.f0.toString()+","+data.f1.toString();
        return s.getBytes();
    }
}

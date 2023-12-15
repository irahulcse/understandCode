package thesis.common.serdes;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import thesis.context.data.ImageData;
import thesis.context.data.LocationData;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

public class CameraDataDeserializer implements KafkaRecordDeserializationSchema<ImageData> {
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<ImageData> out) throws IOException {
        ByteArrayInputStream bin = new ByteArrayInputStream(record.value());
        ObjectInputStream oin = new ObjectInputStream(bin);
        try {
            out.collect((ImageData) oin.readObject());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TypeInformation<ImageData> getProducedType() {
        return TypeInformation.of(ImageData.class);
    }
}

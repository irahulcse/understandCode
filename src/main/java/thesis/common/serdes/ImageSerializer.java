package thesis.common.serdes;

import org.apache.kafka.common.serialization.Serializer;
import thesis.context.data.ImageData;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class ImageSerializer implements Serializer<ImageData> {
    @Override
    public byte[] serialize(String topic, ImageData data) {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oOut = new ObjectOutputStream(bout);
            oOut.writeObject(data);
            return bout.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

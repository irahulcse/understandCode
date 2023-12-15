package thesis.common.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import thesis.common.GlobalConfig;
import thesis.common.serdes.ImageSerializer;
import thesis.context.data.ImageData;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Properties;

public class CameraDataProducer {

    private static final String TOPIC_NAME = GlobalConfig.IMAGE_TOPIC;
    private static final String BOOTSTRAP_SERVERS = GlobalConfig.BOOTSTRAP_SERVER;
    private static final int NUM_RECORDS = 1105;
    private static final String path = GlobalConfig.imageSourcePath;

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ImageSerializer.class.getName());

        long n = 0;

        try (KafkaProducer<String, ImageData> producer = new KafkaProducer<>(props)) {
            while (n < NUM_RECORDS) {
                File file = new File(path + n + ".png");
                ImageData data = new ImageData(Files.readAllBytes(file.toPath()));
                ProducerRecord<String, ImageData> record = new ProducerRecord<>(TOPIC_NAME, data);
                producer.send(record);
                System.out.println("Sent frame " + path + n + ".png at time "+record.timestamp());
                n += 1;
                Thread.sleep(50); // 20fps
            }
        } catch (InterruptedException | IOException e) {
            throw new RuntimeException(e);
        }

    }

}

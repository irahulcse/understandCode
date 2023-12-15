package thesis.common.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import thesis.common.GlobalConfig;

import java.util.Properties;

public class VehicleDataProducer {

    private static final String TOPIC_NAME = GlobalConfig.INPUT_TOPIC;
    private static final String BOOTSTRAP_SERVERS = GlobalConfig.BOOTSTRAP_SERVER;
    private static final int NUM_RECORDS = 3600;

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            double r = 0;
            int num = 0;
            double x, y, v;
            while (num < NUM_RECORDS) {
                // location
                x = Math.round(10 * Math.cos(r) * 100) / 100.0;
                y = Math.round(10 * Math.sin(r) * 100) / 100.0;
                double finalR = r;
                double finalX = x;
                double finalY = y;
                producer.send(new ProducerRecord<>(TOPIC_NAME, null, System.currentTimeMillis(),
                                "location", x + "," + y),
                        (metadata, exception) -> System.out.println("r = " + finalR + " Coordinate: " + finalX + ", " + finalY));
                Thread.sleep(2000);
                r += 0.3;
                // velocity
                if (num < 10) {
                    v = num + Math.round(Math.random() * 100) / 100.0;
                } else {
                    v = 10 + Math.round(Math.random() * 100) / 100.0;
                }
                double finalV = v;
                producer.send(new ProducerRecord<>(TOPIC_NAME, null, System.currentTimeMillis(), "velocity", String.valueOf(v)),
                        (metadata, exception) -> System.out.println("v = " + finalV));
                num++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

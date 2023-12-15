package thesis.common.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import thesis.common.GlobalConfig;

import java.util.Properties;

public class SpeedKafkaProducer {

    private static final String TOPIC_NAME = GlobalConfig.SCALAR_TOPIC;
    private static final String BOOTSTRAP_SERVERS = GlobalConfig.BOOTSTRAP_SERVER;
    private static final int NUM_RECORDS = 3600;

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        double[] sampleValues = {75.0, 78.0, 81.0, 85.0, 87.0, 89.0, 90.0, 91.0, 91.0, 92.0, 92.0, 92.0, 91.0, 90.0, 88.0, 86.0, 84.0, 83.0, 81.0, 78.0, 76.0};
        int length = sampleValues.length;

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            double num = 0;
            while (num < NUM_RECORDS) {
                for (int i = 0; i < length; i++) {
                    int finalI = i;
                    ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, null, System.currentTimeMillis(),
                            "speed", String.valueOf(sampleValues[i]));
                    producer.send(record,
                            (metadata, exception) -> System.out.println("Emitted " + sampleValues[finalI]+ " at "+record.timestamp() ));
                    num++;
                    Thread.sleep(1000);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

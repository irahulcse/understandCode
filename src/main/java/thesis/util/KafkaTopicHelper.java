package thesis.util;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import thesis.common.GlobalConfig;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Programmatically generate necessary topics for the processing jobs.
 * Add names of the topics to the input argument of {@link #collectTopics(Set, String...)} to add more topics.
 */
public class KafkaTopicHelper {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", GlobalConfig.BOOTSTRAP_SERVER);
        String switchingTopic1 = GlobalConfig.SWITCHING_TOPIC + "-" + "app1";
        String switchingTopic2 = GlobalConfig.SWITCHING_TOPIC + "-" + "app2";

        try (AdminClient adminClient = AdminClient.create(properties)) {
            ListTopicsOptions options = new ListTopicsOptions();
            options.listInternal(false);

            ListTopicsResult topics = adminClient.listTopics(options);
            KafkaFuture<Set<String>> topicsNames = topics.names();
            Set<String> names = topicsNames.get();

            List<NewTopic> newTopics = collectTopics(names, GlobalConfig.SWITCHING_TOPIC, switchingTopic1, switchingTopic2);

            CreateTopicsResult createTopicsResult = adminClient.createTopics(newTopics);
            createTopicsResult.all().get();


        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<NewTopic> collectTopics(Set<String> existingNameSet, String... topics){
        List<NewTopic> newTopics = new ArrayList<>();
        for (String topic : topics){
            if (!existingNameSet.contains(topic)){
                NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
                newTopics.add(newTopic);
            }
        }
        return newTopics;
    }
}

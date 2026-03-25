import org.apache.kafka.clients.producer.*;
import java.util.*;

public class KafkaProducerApp {

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.setProperty("max.poll.records", "50");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 5);
        props.put("linger.ms", 10);
        props.put("batch.size", 32768);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        Random random = new Random();
        String[] interactions = {"click", "view", "purchase"};

        while (true) {

            String userId = "user_" + random.nextInt(1000);
            String itemId = "item_" + random.nextInt(500);
            int value = random.nextInt(100);

            String json = String.format(
                    "{\"userId\":\"%s\",\"itemId\":\"%s\",\"interactionType\":\"%s\",\"timestamp\":%d,\"value\":%d}",
                    userId,
                    itemId,
                    interactions[random.nextInt(interactions.length)],
                    System.currentTimeMillis(),
                    value
            );

            ProducerRecord<String, String> record =
                    new ProducerRecord<>("user_interactions", userId, json);

            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("Sent: " + json);
                } else {
                    exception.printStackTrace();
                }
            });

            Thread.sleep(100); // control speed
        }
    }
}
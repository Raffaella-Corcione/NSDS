package it.polimi.middleware.kafka.eval;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

// Group number: 24
// Group members: Capaccioni Chiara, Corcione Raffaella, Cusano Virgilio

// Is it possible to have more than one partition for topics "sensors1" and "sensors2"?
// It is possible, because the consumer computes the sum of records of the same key,
// and it is guaranteed that records of the same key will be stored on the same partition.

// Is there any relation between the number of partitions in "sensors1" and "sensors2"?
// The two topics must have the same number of partitions, because the assignment of partitions to
// consumers within the same consumer group is deterministic and identical only for topics that have
// the same number of partitions, and we want every consumer to work on the same subset of the keys
// for each topic

// Is it possible to have more than one instance of Merger?
// Yes it is, because the processing of messages of different keys is independent.
// If so, what is the relation between their group id?
// The multiple Mergers must have the same group id.

// Is it possible to have more than one partition for topic "merged"?
// It is possible, because all messages of topic "merged" are processed independently.

// Is it possible to have more than one instance of Validator?
// It is possible.
// If so, what is the relation between their group id?
// they must have the same group id because they represent the same logical consumer.

public class Consumers24 {
    public static void main(String[] args) {
        String serverAddr = "localhost:9092";
        int stage = Integer.parseInt(args[0]);
        String groupId = args[1];

        switch (stage) {
            case 0:
                new Merger(serverAddr, groupId).execute();
                break;
            case 1:
                String transactionalId = args[2];
                new Validator(serverAddr, groupId, transactionalId).execute();
                break;
            case 2:
                System.err.println("Wrong stage");
        }
    }

    private static class Merger {
        private final String serverAddr;
        private final String consumerGroupId;

        // TODO: add attributes if necessary

        // TODO: add arguments if necessary
        public Merger(String serverAddr, String consumerGroupId) {
            this.serverAddr = serverAddr;
            this.consumerGroupId = consumerGroupId;
        }

        public void execute() {
            // Consumer
            final Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);

            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());

            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));

            KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(consumerProps);
            List<String> topics = new ArrayList<>();
            topics.add("sensors1");
            topics.add("sensors2");
            consumer.subscribe(topics);

            // Producer
            final Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);

            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());

            final KafkaProducer<String, Integer> producer = new KafkaProducer<>(producerProps);

            Map<String, Integer> lastSensors1 = new HashMap<>();
            Map<String, Integer> lastSensors2 = new HashMap<>();

            while (true) {
                final ConsumerRecords<String, Integer> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
                for (final ConsumerRecord<String, Integer> record : records) {
                    String topic = record.topic();
                    int sum;
                    if(topic.equals("sensors1")) {
                        sum = record.value() + lastSensors2.getOrDefault(record.key(), 0);
                        lastSensors1.put(record.key(), record.value());
                        System.out.println("Merger: received sensors1 for key " + record.key() + " at partition " + record.partition() + ": " + sum);
                    } else {
                        sum = record.value() + lastSensors1.getOrDefault(record.key(), 0);
                        lastSensors2.put(record.key(), record.value());
                        System.out.println("Merger: received sensors2 for key " + record.key() + " at partition " + record.partition() + ": " + sum);
                    }
                    final ProducerRecord<String, Integer> newRecord = new ProducerRecord<>("merged", record.key(), sum);
                    producer.send(newRecord);
                    producer.flush();

                    final Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
                    for (final TopicPartition partition : records.partitions()) {
                        final List<ConsumerRecord<String, Integer>> partitionRecords = records.records(partition);
                        final long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                        map.put(partition, new OffsetAndMetadata(lastOffset + 1));
                    }

                    consumer.commitSync(map);
                }
            }
        }
    }

    private static class Validator {
        private final String serverAddr;
        private final String consumerGroupId;
        private final String transactionalId;

        // TODO: add attributes if needed

        public Validator(String serverAddr, String consumerGroupId, String transactionalId) {
            this.serverAddr = serverAddr;
            this.consumerGroupId = consumerGroupId;
            this.transactionalId = transactionalId;
        }

        public void execute() {
            // Consumer
            final Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);

            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());

            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));

            KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(Collections.singletonList("merged"));

            // Producer
            final Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);

            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());

            producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);

            final KafkaProducer<String, Integer> producer = new KafkaProducer<>(producerProps);
            producer.initTransactions();

            while (true) {
                final ConsumerRecords<String, Integer> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
                for (final ConsumerRecord<String, Integer> record : records) {
                    producer.beginTransaction();
                    producer.send(new ProducerRecord<>("output1", record.key(), record.value()));
                    producer.send(new ProducerRecord<>("output2", record.key(), record.value()));
                    System.out.println("Validator: received merged at key " + record.key() + ": " + record.value());
                    producer.commitTransaction();

                    final Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
                    for (final TopicPartition partition : records.partitions()) {
                        final List<ConsumerRecord<String, Integer>> partitionRecords = records.records(partition);
                        final long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                        map.put(partition, new OffsetAndMetadata(lastOffset + 1));
                    }

                    consumer.commitSync(map);
                }
            }
        }
    }
}

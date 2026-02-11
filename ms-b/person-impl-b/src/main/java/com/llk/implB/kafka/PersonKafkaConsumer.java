package com.llk.implB.kafka;


import com.llk.apiB.*;
import org.apache.kafka.clients.consumer.*;
import com.google.gson.Gson;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;


public class PersonKafkaConsumer  implements Runnable, PersonEventConsumer {

    private KafkaConsumer<String, String> consumer;
    private volatile boolean running = true;
    private Thread thread;

    private PersonService personService;
    private final Gson gson = new Gson();
    private static final String DEFAULT_BOOTSTRAP = "kafka:9092";
    private static final String DEFAULT_TOPIC = "person-events";
    private static final String DEFAULT_GROUP_ID = "person-ms-b";
    private String bootstrapServers;
    private String topic;
    private String groupId;

    public void setPersonService(PersonService personService) {
        this.personService = personService;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }
    @Override
    public void start() {
        Properties props = new Properties();
        String bootstrap = bootstrapServers;
        if (bootstrap == null || bootstrap.trim().isEmpty()) {
            bootstrap = DEFAULT_BOOTSTRAP;
        }
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, resolveGroupId());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);


        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(resolveTopic()));
        thread = new Thread(this, "person-kafka-consumer");
        thread.start();
        System.out.println("Kafka Consumer STARTED: " + bootstrap);

    }

    @Override
    public void stop() {
        running = false;
        if (consumer != null) consumer.wakeup();
        System.out.println("Kafka Consumer STOPPED");
    }

    @Override
    public void run() {
        try {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("MS-B CONSUMED: " + record.value());
                    PersonEvent event = gson.fromJson(record.value(), PersonEvent.class);
                    handle(event);
                }
            }
        } catch (WakeupException e) {
        } finally {
            consumer.close();
        }
    }

    private void handle(PersonEvent event) {
        try {
            switch (event.getType()) {

                case CREATE:
                    personService.create(event.getPerson());
                    System.out.println("CREATE OK");
                    break;

                case UPDATE:
                    personService.update(event.getPerson());
                    System.out.println("UPDATE OK");
                    break;

                case DELETE:
                    personService.delete(event.getId());
                    System.out.println("DELETE OK");
                    break;
            }
        } catch (Exception e) {
            System.err.println("EVENT FAILED: " + e.getMessage());
        }
    }

    private String resolveTopic() {
        if (topic != null && !topic.trim().isEmpty()) return topic;
        return DEFAULT_TOPIC;
    }

    private String resolveGroupId() {
        if (groupId != null && !groupId.trim().isEmpty()) return groupId;
        return DEFAULT_GROUP_ID;
    }
}

package com.llk.impl.kafka;

import com.google.gson.Gson;
import com.llk.api.Person;
import com.llk.api.PersonEvent;
import com.llk.api.PersonEventProducer;
import com.llk.api.PersonEventType;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class PersonKafkaProducer implements PersonEventProducer{

    private KafkaProducer<String, String> producer;
    private final Gson gson = new Gson();
    private static final String DEFAULT_BOOTSTRAP = "kafka:9092";
    private static final String DEFAULT_TOPIC = "person-events";
    private String bootstrapServers;
    private String topic;

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void init() {
        Properties props = new Properties();
        String bootstrap = bootstrapServers;
        if (bootstrap == null || bootstrap.trim().isEmpty()) {
            bootstrap = DEFAULT_BOOTSTRAP;
        }
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        producer = new KafkaProducer<>(props);
        System.out.println("Kafka Producer INIT OK: " + bootstrap);
    }

    public void destroy() {
        if (producer != null) producer.close();
    }

    @Override
    public void sendCreate(Person person) {
        send(PersonEventType.CREATE, person);
    }

    @Override
    public void sendUpdate(Person person) {
        send(PersonEventType.UPDATE, person);
    }

    @Override
    public void sendDelete(Person person) {
        if (person == null || person.getId() <= 0) return;

        PersonEvent event = new PersonEvent(PersonEventType.DELETE, person.getId());
        String resolvedTopic = resolveTopic();
        producer.send(new ProducerRecord<>(
                resolvedTopic,
                String.valueOf(person.getId()),
                gson.toJson(event)
        ));

    }

    private void send(PersonEventType type, Person person) {
        PersonEvent event = new PersonEvent(type, person);
        String resolvedTopic = resolveTopic();
        producer.send(new ProducerRecord<>(
                resolvedTopic,
                String.valueOf(person.getId()),
                gson.toJson(event)
        ));
    }

    private String resolveTopic() {
        if (topic != null && !topic.trim().isEmpty()) return topic;
        return DEFAULT_TOPIC;
    }
}

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

    public void init() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:29092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        producer = new KafkaProducer<>(props);
        System.out.println("Kafka Producer INIT OK");
//        String bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
//        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
//            bootstrapServers = "kafka-1:29092";
//        }
//
//        Properties props = new Properties();
//        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        props.put(ProducerConfig.ACKS_CONFIG, "1");
//
//        producer = new KafkaProducer<>(props);
//        System.out.println("Kafka Producer INIT OK");

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

        producer.send(new ProducerRecord<>(
                "person-events",
                String.valueOf(person.getId()),
                gson.toJson(event)
        ));

    }

    private void send(PersonEventType type, Person person) {
        PersonEvent event = new PersonEvent(type, person);
        producer.send(new ProducerRecord<>(
                "person-events",
                String.valueOf(person.getId()),
                gson.toJson(event)
        ));
    }
}
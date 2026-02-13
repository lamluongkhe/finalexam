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

    public void setPersonService(PersonService personService) {
        this.personService = personService;
    }
    @Override
    public void start() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:29092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "person-ms-b");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);


        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("person-events"));
        thread = new Thread(this, "person-kafka-consumer");
        thread.start();
        System.out.println("Kafka Consumer STARTED");

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
}
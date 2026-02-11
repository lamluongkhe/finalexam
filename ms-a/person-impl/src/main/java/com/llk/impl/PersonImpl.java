package com.llk.impl;

import com.llk.api.Person;
import com.llk.api.PersonService;
import com.llk.impl.kafka.PersonKafkaProducer;
import redis.clients.jedis.Jedis;

import java.util.Map;
import java.util.HashMap;


public class PersonImpl implements PersonService {

    private PersonKafkaProducer kafkaProducer;
    public void setKafkaProducer(PersonKafkaProducer kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    private Jedis jedis;
    public void setJedis(Jedis jedis) {
        this.jedis = jedis;
    }

    @Override
    public Person getById(int id) {
        String redisKey = "person:" + id;

        if (!jedis.exists(redisKey)) {
            return null;
        }

        Map<String, String> map = jedis.hgetAll(redisKey);

        return new Person(
                Integer.parseInt(map.get("id")),
                map.get("name"),
                Integer.parseInt(map.get("age"))
        );
    }

    @Override
    public void create(Person person) {
        kafkaProducer.sendCreate(person);
    }

    @Override
    public void update(Person person) {
        kafkaProducer.sendUpdate(person);
    }

    @Override
    public void delete(int id) {
        Person person = new Person();
        person.setId(id);
        kafkaProducer.sendDelete(person);
    }
}

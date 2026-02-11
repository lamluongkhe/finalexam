package com.llk.implB;

import com.llk.apiB.Person;
import com.llk.apiB.PersonService;
import redis.clients.jedis.Jedis;

import java.util.Map;
import java.util.HashMap;


public class PersonImpl implements PersonService {

    private Jedis jedis;

    public void setJedis(Jedis jedis) {
        this.jedis = jedis;
    }

    private boolean exists(Integer id) {
        return jedis.exists("person:" + id);
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
        String key = "person:" + person.getId();

        if (exists(person.getId())) {
            throw new RuntimeException("Person already exists: " + person.getId());
        }

        Map<String, String> map = new HashMap<>();
        map.put("id", String.valueOf(person.getId()));
        map.put("name", person.getName());
        map.put("age", String.valueOf(person.getAge()));

        jedis.hset(key, map);
    }

    @Override
    public void update(Person person) {
        String key = "person:" + person.getId();
        if (!exists(person.getId())) {
            throw new RuntimeException("Person not found: " + person.getId());
        }
        Map<String, String> map = new HashMap<>();
        map.put("id", String.valueOf(person.getId()));
        map.put("name", person.getName());
        map.put("age", String.valueOf(person.getAge()));

        jedis.hset(key, map);
    }

    @Override
    public void delete(int id) {
        String key = "person:" + id;

        if (!exists(id)) {
            throw new RuntimeException("Person not found: " + id);
        }

        jedis.del(key);
    }
}

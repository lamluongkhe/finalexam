package com.llk.api;
import com.llk.api.Person;

public interface PersonService {
    Person getById(int id);
    void update(Person person);
    void create(Person person);
    void delete(int id);
}

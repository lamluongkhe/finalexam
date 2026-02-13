package com.llk.api;

import com.llk.api.Person;
import com.llk.api.PersonEventType;

public class PersonEvent {

    private PersonEventType type;
    private Person person;
    private Integer id;

    public PersonEvent(PersonEventType type, Integer id) {
        this.type = type;
        this.id = id;
    }
    public PersonEvent() {}

    public PersonEvent(PersonEventType type, Person person) {
        this.type = type;
        this.person = person;
    }

    public PersonEventType getType() {
        return type;
    }

    public Person getPerson() {
        return person;
    }
}

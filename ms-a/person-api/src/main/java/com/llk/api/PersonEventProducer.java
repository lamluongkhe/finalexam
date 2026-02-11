package com.llk.api;

public interface PersonEventProducer {
    void sendCreate(Person person);

    void sendUpdate(Person person);

    void sendDelete(Person person);
}

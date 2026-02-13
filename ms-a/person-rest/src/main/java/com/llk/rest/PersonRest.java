package com.llk.rest;

import com.llk.api.Person;
import com.llk.api.PersonEventProducer;
import com.llk.api.PersonService;
import com.llk.impl.kafka.PersonKafkaProducer;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class PersonRest {

    private PersonService personService;
    private PersonEventProducer kafkaProducer;

    public void setPersonService(PersonService personService) {
        this.personService = personService;
    }

    public void setKafkaProducer(PersonEventProducer kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @GET
    @Path("/{id}")
    public Response get(@PathParam("id") int id) {
        Person p = personService.getById(id);
        if (p == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        return Response.ok(p).build();
    }


    @PUT
    public Response update(Person person) {
        personService.update(person);
        System.out.println("Send update thanh cong...");
        return Response.accepted().build();
    }

    @POST
    public Response create(Person person) {
        personService.create(person);
        System.out.println("Send create thanh cong...");
        return Response.status(Response.Status.CREATED).build();
    }

    @DELETE
    @Path("/{id}")
    public Response delete(@PathParam("id") int id) {
        personService.delete(id);
        System.out.println("Send delete thanh cong...");
        return Response.noContent().build();
    }
}
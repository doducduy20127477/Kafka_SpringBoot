package com.learnkafka.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.producer.LibraryEventsProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.apache.kafka.common.requests.FetchMetadata.log;

@RestController
@Slf4j
public class LibraryEventsController {
    public LibraryEventsProducer libraryEventsProducer;

    public LibraryEventsController (LibraryEventsProducer libraryEventsProducer) {
        this.libraryEventsProducer = libraryEventsProducer;
    }

    @PostMapping("/v1/libraryevent")
    public ResponseEntity<LibraryEvent> postLibraryEvent(
            @RequestBody LibraryEvent libraryEvent
            ) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {

        log.info("libraryevent: {}", libraryEvent);
//        libraryEventsProducer.sendLibraryEvents(libraryEvent);
//        libraryEventsProducer.sendLibraryEvents_approach2(libraryEvent);
        libraryEventsProducer.sendLibraryEvents_approach3(libraryEvent);
        log.info("After sending libraryEvents: ");

        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }
}

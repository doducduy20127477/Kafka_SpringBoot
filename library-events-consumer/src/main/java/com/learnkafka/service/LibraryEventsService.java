package com.learnkafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.jpa.LibraryEventsRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@Slf4j
public class LibraryEventsService {
	@Autowired
	ObjectMapper objectMapper ;
	@Autowired
	private LibraryEventsRepository libraryEventsRepository;
	public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
		LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
		log.info("libraryEvent: {} ", libraryEvent);
		if (libraryEvent != null && libraryEvent.getLibraryEventId() != null && libraryEvent.getLibraryEventId() == 999) {
			throw new RecoverableDataAccessException("Temporary Network Issue");
		}

		switch (libraryEvent.getLibraryEventType()) {
			case NEW:
				save(libraryEvent);
				break;
			case UPDATE:
				//validate
				//update
				validate(libraryEvent);
				save(libraryEvent);
				break;
			default:
				log.info("Invalid Library Event type");
		}
	}

	private void validate (LibraryEvent libraryEvent) {
		// null?
		// exist in db?
		if (libraryEvent.getLibraryEventId() == null) {
			throw new IllegalStateException("Library Event Id is missing");
		}
		Optional<LibraryEvent> libraryEventOptional = libraryEventsRepository.findById(libraryEvent.getLibraryEventId());
		if (!libraryEventOptional.isPresent()) {
			throw new IllegalStateException("Not a valid Library Event");
		}
		log.info("Validation is successful for the library event: {} ", libraryEvent);
	}

	private void save (LibraryEvent libraryEvent) {
		libraryEvent.getBook().setLibraryEvent(libraryEvent);
		libraryEventsRepository.save(libraryEvent);
		log.info("Successfully Persisted the Library Event {} ", libraryEvent);
	}

}

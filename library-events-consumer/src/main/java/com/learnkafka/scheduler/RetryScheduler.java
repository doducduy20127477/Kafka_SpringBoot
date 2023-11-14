package com.learnkafka.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.config.LibraryEventsConsumerConfig;
import com.learnkafka.entity.FailureRecord;
import com.learnkafka.jpa.FailureRecordRepository;
import com.learnkafka.service.LibraryEventsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class RetryScheduler {
    @Autowired
    private FailureRecordRepository failureRecordRepository;
    @Autowired
    private LibraryEventsService libraryEventsService;
    public RetryScheduler (FailureRecordRepository failureRecordRepository, LibraryEventsService libraryEventsService) {
        this.failureRecordRepository = failureRecordRepository;
        this.libraryEventsService = libraryEventsService;
    }
    @Scheduled(fixedRate = 10000)
    public void retryFailedRecords() {
        log.info("Retry Failed Record started!");
        failureRecordRepository.findAllByStatus(LibraryEventsConsumerConfig.RETRY)
                .forEach(failureRecord -> {
                    log.info("Retry Failed Record: {}", failureRecord);
                    var consumerRecord = buildConsumerRecord(failureRecord);
                    try {
                        libraryEventsService.processLibraryEvent(consumerRecord);
                        failureRecord.setStatus(LibraryEventsConsumerConfig.SUCCESS);
                        failureRecordRepository.save(failureRecord);
                    } catch (Exception e) {
                        log.error("Exception in retryFailedRecord: {} ", e.getMessage(), e);
                    }
                });
        log.info("Retry Failed Record completed!");
    }

    private ConsumerRecord<Integer, String> buildConsumerRecord (FailureRecord failureRecord) {
        return new ConsumerRecord<>(failureRecord.getTopic(), failureRecord.getPartition()
                                    , failureRecord.getOffset_value(), failureRecord.getKey_value()
                                    , failureRecord.getErrorRecord()
        );
    }
}

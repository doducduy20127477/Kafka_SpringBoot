package com.learnkafka.config;

import com.learnkafka.service.FailureService;
import lombok.extern.slf4j.Slf4j;
import org.apache.catalina.Container;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

import java.util.ArrayList;
import java.util.List;

@Configuration
@EnableKafka
@Slf4j
public class LibraryEventsConsumerConfig {
	public static final String RETRY = "RETRY";
	public static final String DEAD = "DEAD";
	public static final String SUCCESS = "SUCCESS";
	@Autowired
	FailureService failureService;
	@Autowired
	KafkaProperties kafkaProperties;
	@Autowired
	KafkaTemplate kafkaTemplate;
	@Value("${topics.retry:library-events.RETRY}")
	private String retryTopic;

	@Value("${topics.dlt:library-events.DLT}")
	private String deadLetterTopic;

//	public LibraryEventsConsumerConfig (FailureService failureService, KafkaProperties kafkaProperties, KafkaTemplate kafkaTemplate) {
//		this.failureService = failureService;
//		this.kafkaProperties = kafkaProperties;
//		this.kafkaTemplate = kafkaTemplate;
//	}

	public DeadLetterPublishingRecoverer publishingRecoverer() {
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
				(r, e) -> {
					log.error("Exception in publishingRecoverer : {} ", e.getMessage(), e);
					if (e.getCause() instanceof RecoverableDataAccessException) {
						return new TopicPartition(retryTopic, r.partition());
					}
					else {
						return new TopicPartition(deadLetterTopic, r.partition());
					}
				});
		return recoverer;
	}
	ConsumerRecordRecoverer consumerRecordRecoverer = (consumerRecord, e) -> {
		log.error("Exception in consumerRecordRecoverer : {} ", e.getMessage(), e);
		var record = (ConsumerRecord<Integer, String>) consumerRecord;
		if (e.getCause() instanceof RecoverableDataAccessException) {
			log.info("Inside Recovery");
			failureService.saveFailureRecord(record, e, RETRY);
		}
		else {
			log.info("Inside Non-Recovery");
			failureService.saveFailureRecord(record, e, DEAD);
		}
	};
	@Bean
	ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
			ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
			ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory,
			ObjectProvider<ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>>> kafkaContainerCustomizer) {
		ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
		configurer.configure(factory, kafkaConsumerFactory
				                              .getIfAvailable(() -> new DefaultKafkaConsumerFactory<>(this.kafkaProperties.buildConsumerProperties())));
		kafkaContainerCustomizer.ifAvailable(factory::setContainerCustomizer);
		factory.setConcurrency(3);
		factory.setCommonErrorHandler(errorHandler());
//		factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
		return factory;
	}

	private DefaultErrorHandler errorHandler () {
		var exceptionToIgnoreList = List.of(
				IllegalAccessException.class
		);
		var exceptionToRetryList = List.of(
				RecoverableDataAccessException.class
		);
		var fixedBackOff = new FixedBackOff(1000L, 2L);
		var expBackOff = new ExponentialBackOffWithMaxRetries(2);
		expBackOff.setInitialInterval(1_000L);
		expBackOff.setMultiplier(2.0);
		expBackOff.setMaxInterval(2_000L);
		DefaultErrorHandler errorHandler = new DefaultErrorHandler(
				//publishingRecoverer(),
				consumerRecordRecoverer,
				fixedBackOff
				//expBackOff
		);
		exceptionToIgnoreList.forEach(errorHandler:: addNotRetryableExceptions);
		//exceptionToRetryList.forEach(errorHandler:: addRetryableExceptions);
		errorHandler
				.setRetryListeners((record, ex, deliveryAttempt) -> {
					log.info("Failed Record in Retry Listener, Exception: {} , deliveryAttempt: {} "
							, ex.getMessage(), deliveryAttempt);
				});
		return errorHandler;
	}

}

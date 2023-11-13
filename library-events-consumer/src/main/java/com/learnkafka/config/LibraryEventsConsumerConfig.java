package com.learnkafka.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.catalina.Container;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
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
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

import java.util.ArrayList;
import java.util.List;

@Configuration
@EnableKafka
@Slf4j
public class LibraryEventsConsumerConfig {
	
	@Autowired
	KafkaProperties kafkaProperties;
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
		var fixedBackOff = new FixedBackOff(1000L, 2);
		var expBackOff = new ExponentialBackOffWithMaxRetries(2);
		expBackOff.setInitialInterval(1_000L);
		expBackOff.setMultiplier(2.0);
		expBackOff.setMaxInterval(2_000L);
		DefaultErrorHandler errorHandler = new DefaultErrorHandler(
				//fixedBackOff
				expBackOff
		);
//		exceptionToIgnoreList.forEach(errorHandler:: addNotRetryableExceptions);
		exceptionToRetryList.forEach(errorHandler:: addRetryableExceptions);
		errorHandler
				.setRetryListeners((record, ex, deliveryAttempt) -> {
					log.info("Failed Record in Retry Listener, Exception: {} , deliveryAttempt: {} "
							, ex.getMessage(), deliveryAttempt);
				});
		return errorHandler;
	}

}

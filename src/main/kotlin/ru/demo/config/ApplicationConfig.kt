package ru.demo.config

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.task.SimpleAsyncTaskExecutor
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.config.KafkaListenerContainerFactory
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.kafka.support.JacksonUtils
import org.springframework.kafka.support.serializer.JsonDeserializer
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.scheduling.concurrent.ConcurrentTaskExecutor
import ru.demo.model.StringValue
import ru.demo.service.StringValueConsumer


@Configuration
class ApplicationConfig(@param:Value("\${application.kafka.topic}") val topicName: String) {
    @Bean
    fun objectMapper(): ObjectMapper {
        return JacksonUtils.enhancedObjectMapper()
    }

    @Bean
    fun consumerFactory(
        kafkaProperties: KafkaProperties, mapper: ObjectMapper,
    ): ConsumerFactory<String, StringValue> {
        val props = kafkaProperties.buildConsumerProperties()
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = JsonDeserializer::class.java
        props[JsonDeserializer.TYPE_MAPPINGS] = "ru.demo.model.StringValue:ru.demo.model.StringValue"
        props[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = 3
        props[ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG] = 3000
        val kafkaConsumerFactory = DefaultKafkaConsumerFactory<String, StringValue>(props)
        kafkaConsumerFactory.setValueDeserializer(JsonDeserializer(mapper))
        return kafkaConsumerFactory
    }

    @Bean("listenerContainerFactory")
    fun listenerContainerFactory(consumerFactory: ConsumerFactory<String, StringValue>): KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, StringValue>> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, StringValue>()
        factory.consumerFactory = consumerFactory
        factory.isBatchListener = true
        factory.setConcurrency(1)
        factory.containerProperties.idleBetweenPolls = 1000
        factory.containerProperties.pollTimeout = 1000
        val executor = SimpleAsyncTaskExecutor("k-consumer-")
        executor.concurrencyLimit = 10
        val listenerTaskExecutor = ConcurrentTaskExecutor(executor)
        factory.containerProperties.listenerTaskExecutor = listenerTaskExecutor
        return factory
    }

    @Bean
    fun topic(): NewTopic {
        return TopicBuilder.name(topicName).partitions(1).replicas(1).build()
    }

    @Bean
    fun stringValueConsumer(stringValueConsumer: StringValueConsumer): KafkaClient {
        return KafkaClient(stringValueConsumer)
    }

    class KafkaClient(private val stringValueConsumer: StringValueConsumer) {
        @KafkaListener(topics = ["\${application.kafka.topic}"], containerFactory = "listenerContainerFactory")
        fun listen(@Payload values: List<StringValue>) {
            log.info("values, values.size:{}", values.size)
            stringValueConsumer.accept(values)
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(ApplicationConfig::class.java)
    }
}
package com.ksb.consumer;

import com.ksb.config.AppConfig;
import com.ksb.dto.CityInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class KafkaConsumer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    @KafkaListener(
            groupId = AppConfig.GROUPID,
            topics = AppConfig.TOPIC,
            containerFactory = AppConfig.KAFKA_LISTENER_CONTAINER_FACTORY
    )
    public void receivedMessage(CityInfo message) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        String jsonString = mapper.writeValueAsString(message);
        logger.info("Json message received using Kafka listener :  {}" , jsonString);
    }
}

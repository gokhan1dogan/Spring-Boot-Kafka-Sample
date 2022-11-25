package com.ksb.producer;

import com.ksb.config.AppConfig;
import com.ksb.dto.CityInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/produce")
public class KafkaProducer {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @PostMapping("/message")
    public String sendMessage(@RequestBody CityInfo message) {
        try {
            kafkaTemplate.send(AppConfig.TOPIC, message);
        } catch (Exception e){
            e.printStackTrace();
        }
        return "Json message is successfully sent";
    }
}

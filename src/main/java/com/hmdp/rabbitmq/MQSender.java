package com.hmdp.rabbitmq;

import com.hmdp.config.RabbitMQConfig;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class MQSender {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    public void sendSeckillMessageToQueue(String message){

        rabbitTemplate.convertAndSend(RabbitMQConfig.EXCHANGE, "seckill.message", message);
    }
}

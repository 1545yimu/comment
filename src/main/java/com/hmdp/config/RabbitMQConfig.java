package com.hmdp.config;


import org.springframework.amqp.core.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfig {

    public static final String EXCHANGE = "seckill.exchange";

    public static final String Queue = "seckill.queue";

    public static final String ROUTINGKEY = "seckill.#";


    //声明交换机
    //FanoutExchange类:广播交换机
    //DirectExchange类:路由交换机，将接收到的消息根据规则路由到指定的队列
    //topicExchange类:话题交换机
    @Bean
    public TopicExchange topicExchange(){
        return new TopicExchange(EXCHANGE);
    }

    //声明队列
    @Bean
    public Queue queue(){
        return new Queue(Queue);
    }

    //绑定队列和交换机
    @Bean
    public Binding binding(TopicExchange topicExchange, Queue queue){
        return BindingBuilder.bind(queue).to(topicExchange).with(ROUTINGKEY);
    }

}

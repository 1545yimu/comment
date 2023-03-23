package com.hmdp.rabbitmq;

import cn.hutool.json.JSONUtil;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.IVoucherOrderService;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class MQListener {

    @Autowired
    IVoucherOrderService voucherOrderService;

    //接收RabbitMQ中的消息
    //监听"seckill.queue"队列、路由key为"seckill.#"
    @RabbitListener(queues = "seckill.queue")
    public void listenQueueSeckillMessage(String message){
        VoucherOrder voucherOrder = JSONUtil.toBean(message, VoucherOrder.class);

        voucherOrderService.createVoucherOrder(voucherOrder);
    }
}

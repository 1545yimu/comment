package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.rabbitmq.MQSender;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 *  抢购秒杀优惠卷服务实现类
 * </p>
 *
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    //代理对象
    //IVoucherOrderService proxy;

    @Resource
    private MQSender mqSender;


    //加载lua脚本
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    //创建阻塞队列
    //阻塞队列特点：当一个线程尝试从队列中获取元素，没有元素，线程就会被阻塞，直到队列中有元素，线程才会被唤醒，并去获取元素
    /*
        使用JVM的阻塞队列实现异步秒杀有两个问题：
            （1）JVM的内存限制问题
            （2）数据安全问题（JVM内存没有持久化机制）
        为了解决以上问题，可使用MQ作为消息队列
     */
    //private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);

    //创建线程池
    //private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    //让类初始化完成后就开始执行VoucherOrderHandler
//    @PostConstruct
//    private void init() {
//        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
//    }
//
//    private class VoucherOrderHandler implements Runnable{
//
//        @Override
//        public void run() {
//            while (true){
//                try {
//                    // 1.获取阻塞队列中获取订单信息
//                    VoucherOrder voucherOrder = orderTasks.take();
//                    // 2.创建订单
//                    handleVoucherOrder(voucherOrder);
//                } catch (Exception e) {
//                    log.error("处理订单异常", e);
//                }
//            }
//        }
//    }

    //秒杀业务优化:创建订单方法，由线程池调用
    /*public void handleVoucherOrder(VoucherOrder voucherOrder){
        //获取用户
        Long userId = voucherOrder.getUserId();
        //创建锁对象
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //获取锁
        boolean isLock = lock.tryLock();
        //判断是否获取锁成功
        if(!isLock){
            log.error("不允许重复下单");
            return;
        }
        try{
            //调用代理对象中createVoucherOrder()完成mysql扣减库存、保存订单信息的操作
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            lock.unlock();
        }
    }
    */

    //秒杀业务优化:创建订单方法，由MQ调用
    public void createVoucherOrder(VoucherOrder voucherOrder){
        //获取用户
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();

        //创建锁对象
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //获取锁
        boolean isLock = lock.tryLock();
        //判断是否获取锁成功
        if(!isLock){
            log.error("不允许重复下单");
            return;
        }


        try {
            //一人一单实现
            //根据用户id和优惠卷id，判断用户是否已经下单
            //不同线程的ThreadLocal不同，异步执行时不能这样获取
            //Long userId = UserHolder.getUser().getId();

            int count = query()
                    .eq("user_id", userId)
                    .eq("voucher_id", voucherId)
                    .count();
            //如果该用户已经下过单
            if (count > 0) {
                log.error("用户已经购买过了");
                return;
            }

            //5.扣减数据库中库存
            boolean success = seckillVoucherService
                    .update()
                    .setSql("stock = stock - 1")
                    .eq("voucher_id", voucherId)
                    //解决超卖问题，乐观锁一般在更新数据的时候使用
                    //使用乐观锁，以库存为版本号，若判断前后两次版本号是否相等，会出现只有一个线程能正常执行，其他所有线程抢不到的问题
                    //优化为判断库存是否大于0
                    .gt("stock", 0)
                    .update();
            if (!success) {
                //扣减失败
                log.error("库充不足");
                return;
            }

            //保存订单信息到数据库
            save(voucherOrder);
        }finally {
            //释放锁
            lock.unlock();
        }
    }


    /**
     *  秒杀业务优化思路：
     * （1）使用Redis，基于Lua脚本，判断秒杀库存、一人一单，决定用户是否抢购成功
     * （2）如果抢购成功，开启线程任务，不断从阻塞队列中获取信息，实现异步下单功能，完成mysql扣减库存、保存订单信息的操作
     * 改进操作（2）：使用MQ作为消息队列，实现异步下单功能
     * @return Result.ok(订单id)
     **/
    //秒杀业务优化:使用Redis，基于Lua脚本，判断秒杀库存、一人一单，决定用户是否抢购成功,如果成功放入阻塞队列中
    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();

        //执行seckill.lua脚本,保证Redis操作的原子性
        int execute = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString()
        ).intValue();

        if(execute != 0)
            return Result.fail(execute == 1 ? "库存不足" : "不能重复下单");

        //如果脚本返回值为0，把下单信息保存到MQ
        long orderId = redisIdWorker.nextId("order");
        VoucherOrder voucherOrder = new VoucherOrder();
        //设置订单id
        voucherOrder.setId(orderId);
        //设置用户id
        voucherOrder.setUserId(UserHolder.getUser().getId());
        //设置优惠卷id
        voucherOrder.setVoucherId(voucherId);

        //voucherOrder放入阻塞队列
        /*
            使用JVM的阻塞队列实现异步秒杀有两个问题：
                （1）JVM的内存限制问题
                （2）数据安全问题（JVM内存没有持久化机制）
            为了解决以上问题，可使用MQ作为消息队列
        */
        //orderTasks.add(voucherOrder);

        //获取代理对象
        //不同线程的ThreadLocal不同，为了能够异步执行事务方法，需要提前获取代理对象
        //proxy = (IVoucherOrderService)AopContext.currentProxy();

        //voucherOrder放入RabbitMQ
        mqSender.sendSeckillMessageToQueue(JSONUtil.toJsonStr(voucherOrder));

        //返回订单id
        return Result.ok(orderId);
    }


      //秒杀业务逻辑
//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        //1.查询优惠卷
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//
//        //2.判断秒杀是否开始
//        if(voucher.getBeginTime().isAfter(LocalDateTime.now())){
//            //尚未开始
//            return Result.fail("秒杀尚未开始");
//        }
//
//        //3.判断秒杀是否已经结束
//        if(voucher.getEndTime().isBefore(LocalDateTime.now())){
//            //秒杀已经结束
//            return Result.fail("秒杀已经结束");
//        }
//
//        //4.判断库存是否充足
//        if (voucher.getStock() < 1) {
//            return Result.fail("库充不足");
//        }
//
//        Long userId = UserHolder.getUser().getId();
//
//        //事务提交之后，才能释放锁，所以锁要加在这里
//        //锁的是同一个用户，不同用户不用加锁，提高性能
//
//        //使用自己编写的分布式锁,创建锁对象
//        /*
//           自己编写的分布式锁，存在以下问题：
//            (1)不可重入：同一个线程无法多次获取同一把锁
//            (2)不可重试：获取锁只尝试一次就返回false，没有重试机制
//            (3)超时释放：锁超时释放虽然可以避免死锁，但如果是业务执行耗时较长，也会导致锁释放，存在安全隐患
//            (4)主从一致性：如果Redis提供了主从集群，主从同步存在延迟，当主宕机时，如果从结点并未同步锁数据，会导致其他线程获取锁成功
//           建议使用Redisson（开源项目）代替
//        */
//        //SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
//
//        //使用Redisson,创建锁对象(Redisson原理自行了解)
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//
//        //尝试获取锁
//        boolean isLock = lock.tryLock();
//        //如果获取锁不成功
//        if(!isLock){
//            //返回错误信息
//            return Result.fail("不允许重复下单");
//        }
//
//        /* synchronized只能保证单个JVM内部的多个线程之间的互斥,没有办法让集群下的多个JVM线程之间互斥 */
//        //synchronized (userId.toString().intern()) {
//        try {
//            //为了让createVoucherOrder()事务生效，需要拿到当前对象的代理对象，使用代理对象去调用事务方法
//            IVoucherOrderService proxy = (IVoucherOrderService)AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        } finally {
//            lock.unlock();
//        }
//        //}
//    }

//    @Transactional
//    public Result createVoucherOrder(Long voucherId){
//        //一人一单实现
//        //根据用户id和优惠卷id，判断用户是否已经下单
//        Long userId = UserHolder.getUser().getId();
//
//        int count = query()
//                .eq("user_id", userId)
//                .eq("voucher_id", voucherId)
//                .count();
//        //如果该用户已经下过单
//        if (count > 0) {
//            return Result.fail("用户已经购买过了");
//        }
//
//        //5.扣减库存
//        boolean success = seckillVoucherService
//                .update()
//                .setSql("stock = stock - 1")
//                .eq("voucher_id", voucherId)
//                //解决超卖问题，乐观锁一般在更新数据的时候使用
//                //使用乐观锁，以库存为版本号，若判断前后两次版本号是否相等，会出现只有一个线程能正常执行，其他所有线程抢不到的问题
//                //优化为判断库存是否大于0
//                .gt("stock", 0)
//                .update();
//        if (!success) {
//            //扣减失败
//            return Result.fail("库充不足");
//        }
//
//        //6.创建订单
//        VoucherOrder voucherOrder = new VoucherOrder();
//        //设置订单id
//        long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//        //设置用户id
//        voucherOrder.setVoucherId(UserHolder.getUser().getId());
//        //设置优惠卷id
//        voucherOrder.setVoucherId(voucherId);
//        //保存订单信息到数据库
//        save(voucherOrder);
//
//        return Result.ok(orderId);
//    }
}

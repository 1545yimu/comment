package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.geo.Distance;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        //缓存穿透
        //Shop shop = queryWithPassThrough(id);

        //缓存穿透，缓存无效key + 缓存击穿,使用互斥锁
        //Shop shop = queryWithMutex(id);

        //缓存击穿,使用逻辑过期
        //Shop shop = queryWithLogicalExpire(id);

        //使用封装工具类，解决缓存穿透
        Shop shop = cacheClient.queryWithPassThrough(
                RedisConstants.CACHE_SHOP_KEY, id, Shop.class, this::getById, RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES
        );

        if (shop == null) {
            return Result.fail("店铺不存在!");
        }

        //7.返回
        return Result.ok(shop);
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    //缓存击穿,使用逻辑过期
    private Shop queryWithLogicalExpire(Long id){
        String key = RedisConstants.CACHE_SHOP_KEY + id;

        //1.从Redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        //2.判断是否存在
        if(StrUtil.isBlank(shopJson)){
            //3.缓存不存在，直接返回
            return null;
        }

        //缓存中存在,需要先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();

        //判断对象是否过期
        if(expireTime.isAfter(LocalDateTime.now())){
            //未过期，直接返回店铺信息
            return shop;
        }

        //已过期，需要缓存重建
        //获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);

        //如果成功获取锁，开启独立线程，实现缓存重建
        if(isLock){
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    this.saveShop2Redis(id, 20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    //释放互斥锁
                    unLock(lockKey);
                }
            });
        }

        //返回已过期的商铺信息
        return shop;
    }


    //缓存穿透，缓存无效key + 缓存击穿,使用互斥锁
    private Shop queryWithMutex(Long id){
        String key = RedisConstants.CACHE_SHOP_KEY + id;

        //1.从Redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        //2.判断是否存在
        if(StrUtil.isNotBlank(shopJson)){   //如果shopJson不为null，不为空字符串  返回true
            //3.缓存中存在，直接返回
            return JSONUtil.toBean(shopJson, Shop.class);
        }

        //程序若走到这一步，说明：shopJson为null或空字符串
        //如果shopJson不为null，说明shopJson为空值
        if(shopJson != null){
            //return Result.fail("店铺信息不存在");
            return null;
        }

        //互斥锁实现缓存击穿
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        Shop shop = null;

        try {
            //获取互斥锁
            boolean isLock = tryLock(lockKey);

            //获取锁不成功
            if(!isLock){
                //等待，再次获取
                Thread.sleep(50);
                return queryWithMutex(id);
            }

            //4.获取锁成功根据id查询数据库
            shop = getById(id);

            //5.如果数据库中不存在，返回错误
            if(shop == null){
                //为解决缓存穿透的问题，将空值写入redis
                stringRedisTemplate.opsForValue()
                        .set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);

                //return Result.fail("店铺不存在");
                return null;
            }

            //6.如果数据库中存在，写入redis
            stringRedisTemplate.opsForValue()
                    .set(key, JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            //释放互斥锁
            unLock(lockKey);
        }

        //7.返回
        return shop;
    }


    //缓存穿透，缓存无效key
    private Shop queryWithPassThrough(Long id){
        String key = RedisConstants.CACHE_SHOP_KEY + id;

        //1.从Redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        //2.判断是否存在
        if(StrUtil.isNotBlank(shopJson)){   //如果shopJson不为null，不为空字符串  返回true
            //3.缓存中存在，直接返回

            return JSONUtil.toBean(shopJson, Shop.class);
        }

        //程序若走到这一步，说明：shopJson为null或空字符串
        //如果shopJson不为null，说明shopJson为空值
        if(shopJson != null){
            //return Result.fail("店铺信息不存在");
            return null;
        }

        //4.如果缓存中不存在，根据id查询数据库
        Shop shop = getById(id);

        //5.如果数据库中不存在，返回错误
        if(shop == null){
            //为解决缓存穿透的问题，将空值写入redis
            stringRedisTemplate.opsForValue()
                    .set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);

            //return Result.fail("店铺不存在");
            return null;
        }

        //6.如果数据库中存在，写入redis
        stringRedisTemplate.opsForValue()
                .set(key, JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);

        //7.返回
        return shop;
    }

    private boolean tryLock(String key){
        Boolean aBoolean = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", RedisConstants.LOCK_SHOP_TTL, TimeUnit.MINUTES);

        return BooleanUtil.isTrue(aBoolean);
    }

    private void unLock(String key){
        stringRedisTemplate.delete(key);

    }

    public void saveShop2Redis(Long id, Long expireSeconds){
        //1.查询店铺数据
        Shop shop = getById(id);

        //2.封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));

        //3.写入Redis
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));

    }


    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();

        if(id == null){
            return Result.fail("店铺id不能为空");
        }

        //1.更新数据库
        updateById(shop);

        //2.删缓存
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY + id);

        return Result.ok();
    }

    /**
     * 显示附近商铺
     * @param typeId 商铺类型
     * @param current 页码
     * @param x 经度
     * @param y 纬度
     * @return 商铺列表
     */
    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        //是否需要根据坐标查询
        if(x == null || y == null){
            //不需要坐标查询，直接使用数据库查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            return Result.ok(page.getRecords());
        }

        //计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;

        //查询redis，按照距离排序、分页，结果：shopId、distance
        String key = RedisConstants.SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
                .search(
                        key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
                );

        //解析出id
        if(results == null){
            return Result.ok(Collections.emptyList());
        }

        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();

        if (list.size() <= from) {
            // 没有下一页了，结束
            return Result.ok(Collections.emptyList());
        }

        //截取 from ~ end 的部分
        List<Long> ids = new ArrayList<>(list.size());
        Map<String, Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> {
            //获取店铺id
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            //获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr, distance);
        });

        //根据id查询shop
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }

        // 返回
        return Result.ok(shops);
    }
}

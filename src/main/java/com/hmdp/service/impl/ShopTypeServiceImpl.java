package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryTypeList() {
        //从redis中查询店铺类型
        String cache = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_TYPE_KEY);

        //判断是否存在
        if (StrUtil.isNotBlank(cache)) {
            //如果存在，直接返回
            List<ShopType> shopTypes = JSONUtil.toList(cache, ShopType.class);

            return Result.ok(shopTypes);
        }

        //如果缓存未命中，查数据库
        List<ShopType> shopTypes = query().orderByAsc("sort").list();

        //如果不存在
        if(shopTypes == null || shopTypes.size() == 0){
            return Result.fail("数据不存在");
        }


        //如果数据库中存在，写入redis
        stringRedisTemplate.opsForValue()
                .set(RedisConstants.CACHE_SHOP_TYPE_KEY, JSONUtil.toJsonStr(shopTypes), RedisConstants.CACHE_SHOP_TYPE_TTL, TimeUnit.MINUTES);

        return Result.ok(shopTypes);
    }
}

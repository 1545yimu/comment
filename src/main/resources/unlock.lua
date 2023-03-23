-- 比较线程标示与锁中的标示是否一致
-- 只能当前线程释放自己的锁
if(redis.call('get', KEYS[1]) ==  ARGV[1]) then
    -- 释放锁 del key
    return redis.call('del', KEYS[1])
end
return 0
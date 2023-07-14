--1. 检查是不是你的锁
--2. 删除
-- KEYS[1] 就是你的分布式锁的key
-- ARGV[1] 就是你预期的存在redis 里面的 value
-- ARGV[2] 新的过期时间
if redis.call('get', KEYS[1]) == ARGV[1] then
    --    确实是你的锁
    return redis.call('EXPIRE', KEYS[1], ARGV[2])
else
    --    不是你的锁
    return redis.call('get', KEYS[1])
end
package org.floatsliang.dumblock;

import com.google.common.collect.Lists;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * @author floatsliang@gmail.com
 * @date 2020/5/12 2:21 PM
 */
public class RedisLockInfo extends LockInfo {

    private String lockVal;

    private volatile long redisKeyExpireTs;

    protected int rechargeCount = 0;

    public RedisLockInfo(String lockKey, String lockVal, long timeout, long expireTimeout) {
        super(lockKey, timeout);
        this.lockVal = lockVal;
        this.redisKeyExpireTs = System.currentTimeMillis() + expireTimeout;
    }

    public boolean lockInRedis(Jedis jedis) {
        String lua_scripts = "if redis.call('setnx',KEYS[1],ARGV[1]) == 1 then \n" +
                "\tredis.call('expireat',KEYS[1],ARGV[2]);\n" +
                "\treturn 1;\n" +
                "else \n" +
                "\treturn 0;\n" +
                "end;";
        List<String> keys = Lists.newArrayList(this.getLockKey());
        List<String> args = Lists.newArrayList(this.getLockVal(), String.valueOf(this.getRedisKeyExpireTs() / 1000));
        return jedis.eval(lua_scripts, keys, args).equals(1L);
    }

    public boolean unLockInRedis(Jedis jedis) {
        String luaScript = "if redis.call('get',KEYS[1]) == ARGV[1] then \n" +
                "\treturn redis.call('del',KEYS[1]); \n" +
                "else \n" +
                "\treturn 0; \n" +
                "end;";
        return jedis.eval(luaScript, Lists.newArrayList(this.getLockKey()),
                Lists.newArrayList(this.getLockVal())).equals(1L);
    }

    public boolean rechargeInRedis(Jedis jedis, long expireTimeout) {
        if (this.isReleased()) {
            return true;
        }
        this.rechargeCount++;
        this.setRedisKeyExpireTs(this.getRedisKeyExpireTs() + expireTimeout);
        String luaScript = "if redis.call('get',KEYS[1]) == ARGV[1] then \n" +
                "\treturn redis.call('expireat',KEYS[1], ARGV[2]); \n" +
                "else \n" +
                "\treturn 0; \n" +
                "end;";
        List<String> keys = Lists.newArrayList(this.getLockKey());
        List<String> args = Lists.newArrayList(this.getLockVal(), String.valueOf(this.getRedisKeyExpireTs() / 1000));
        return jedis.eval(luaScript, keys, args).equals(1L);
    }

    public String getLockVal() {
        return lockVal;
    }

    public void setLockVal(String lockVal) {
        this.lockVal = lockVal;
    }

    public long getRedisKeyExpireTs() {
        return redisKeyExpireTs;
    }

    public void setRedisKeyExpireTs(long redisKeyExpireTs) {
        this.redisKeyExpireTs = redisKeyExpireTs;
    }

    public boolean isExpired() {
        return System.currentTimeMillis() > redisKeyExpireTs;
    }

    public int getRechargeCount() {
        return rechargeCount;
    }

    public void setRechargeCount(int rechargeCount) {
        this.rechargeCount = rechargeCount;
    }
}


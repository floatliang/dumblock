package org.floatsliang.dumblock;

import com.google.common.collect.Lists;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * @author floatsliang@gmail.com
 * @date 2020/5/21 3:16 PM
 */
public class RedisSemaphoreInfo extends RedisLockInfo {

    private int allocatedPermits;
    private int maxPermits;

    public RedisSemaphoreInfo(String lockKey, String lockVal, int maxPermits, long timeout, long expireTimeout) {
        super(lockKey, lockVal, timeout, expireTimeout);
        this.maxPermits = maxPermits;
    }

    public int allocatePermits(int permits) {
        int targetPermits = this.allocatedPermits + permits;
        if (targetPermits < 0 || targetPermits > this.maxPermits) {
            throw new IllegalArgumentException(String.format("Illegal permits %s allocated to semaphore, " +
                    "current is %s, newly allocated is %s", targetPermits, this.allocatedPermits, permits));
        }
        this.allocatedPermits = targetPermits;
        return this.allocatedPermits;
    }

    public boolean isReadyToRelease() {
        return this.allocatedPermits <= 0;
    }

    public int allocatedPermits() {
        return this.allocatedPermits;
    }

    @Override
    public void release() {
        if (this.isReadyToRelease()) {
            super.release();
        }
    }

    @Override
    public boolean lockInRedis(Jedis jedis) {
        String luaScript = "if (redis.call('EXISTS', KEYS[1]) == 1) then\n" +
                "\tlocal keyVals = redis.call('HGETALL', KEYS[1]);\n" +
                "\tlocal count = 0;\n" +
                "\tfor key, val in pairs(keyVals) do\n" +
                "\t\tif (key ~= ARGV[1]) then\n" +
                "\t\t\tif (redis.call('EXISTS', key) == 1) then\n" +
                "\t\t\t\tcount = count + val;\n" +
                "\t\t\telse\n" +
                "\t\t\t\tredis.call('HDEL', KEYS[1], key);\n" +
                "\t\t\tend;\n" +
                "\t\tend;\n" +
                "\tend;\n" +
                "\tredis.call('SET', ARGV[1], 1);\n" +
                "\tredis.call('EXPIREAT', ARGV[1], ARGV[4]);\n" +
                "\tif (count + ARGV[2] <= ARGV[3]) then\n" +
                "\t\tredis.call('HSET', KEYS[1], ARGV[1], ARGV[2]);\n" +
                "\t\treturn 1;\n" +
                "\telse\n" +
                "\t\treturn 0;\n" +
                "\tend;\n" +
                "else\n" +
                "\tif (ARGV[2] <= ARGV[3]) then\n" +
                "\t\tredis.call('HSET', KEYS[1], ARGV[1], ARGV[2]);\n" +
                "\t\tredis.call('SET', ARGV[1], 1);\n" +
                "\t\tredis.call('EXPIREAT', ARGV[1], ARGV[4]);\n" +
                "\t\treturn 1;\n" +
                "\telse\n" +
                "\t\treturn 0;\n" +
                "\tend;\n" +
                "end;";
        List<String> keys = Lists.newArrayList(this.getLockKey());
        List<String> args = Lists.newArrayList(this.getLockVal(), String.valueOf(this.allocatedPermits),
                String.valueOf(this.maxPermits), String.valueOf(this.getRedisKeyExpireTs() / 1000));
        return jedis.eval(luaScript, keys, args).equals(1L);
    }

    @Override
    public boolean unLockInRedis(Jedis jedis) {
        String luaScript = "if (redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1) then\n" +
                "\tif (ARGV[2] == 0) then\n" +
                "\t\tredis.call('HDEL', KEYS[1], ARGV[1]);\n" +
                "\t\tredis.call('DEL', ARGV[1]);\n" +
                "\t\tif (redis.call('HLEN', KEYS[1]) == 0) then\n" +
                "\t\t\tredis.call('DEL', KEYS[1]);\n" +
                "\t\tend;\n" +
                "\t\treturn 1;\n" +
                "\telse\n" +
                "\t\tredis.call('HSET', KEYS[1], ARGV[1], ARGV[2]);\n" +
                "\t\treturn 1;\n" +
                "\tend;\n" +
                "else\n" +
                "\treturn 0;\n" +
                "end;";
        List<String> keys = Lists.newArrayList(this.getLockKey());
        List<String> args = Lists.newArrayList(this.getLockVal(), String.valueOf(this.allocatedPermits));
        return jedis.eval(luaScript, keys, args).equals(1L);
    }

    @Override
    public boolean rechargeInRedis(Jedis jedis, long expireTimeout) {
        if (this.isReleased()) {
            return true;
        }
        this.rechargeCount++;
        this.setRedisKeyExpireTs(this.getRedisKeyExpireTs() + expireTimeout);
        String luaScript = "if (redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1) then\n" +
                "\treturn redis.call('EXPIREAT', KEYS[1], ARGV[2]);\n" +
                "else\n" +
                "\treturn 0;\n" +
                "end;";
        List<String> keys = Lists.newArrayList(this.getLockKey());
        List<String> args = Lists.newArrayList(this.getLockVal(), String.valueOf(this.getRedisKeyExpireTs() / 1000));
        return jedis.eval(luaScript, keys, args).equals(1L);
    }
}

package org.floatsliang.dumblock;

import com.google.common.collect.Lists;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * @author floatsliang@gmail.com
 * @date 2020/5/14 2:28 PM
 */
public class RedisWriteLockInfo extends RedisLockInfo {

    public RedisWriteLockInfo(String lockKey, String lockVal, long timeout, long expireTimeout) {
        super(lockKey, lockVal, timeout, expireTimeout);
    }

    @Override
    public String getLockVal() {
        return super.getLockVal() + ":write";
    }

    @Override
    public boolean lockInRedis(Jedis jedis) {
        String luaScript = "local mode = redis.call('HGET', KEYS[1], 'mode');\n" +
                "if (mode == false) then\n" +
                "\tredis.call('HSET', KEYS[1], 'mode', 'write');\n" +
                "\tredis.call('HSET', KEYS[1], ARGV[1], 1);\n" +
                "\tredis.call('HSET', KEYS[1], 'count', 1);\n" +
                "\tredis.call('EXPIREAT', KEYS[1], ARGV[2]);\n" +
                "\treturn 1;\n" +
                "end;\n" +
                "if (mode == 'write') then\n" +
                "\tif (redis.call('HGET', KEYS[1], 'count') == 0) then\n" +
                "\t\tredis.call('HSET', KEYS[1], ARGV[1], 1);\n" +
                "\t\tredis.call('HINCRBY', KEYS[1], 'count', 1);\n" +
                "\t\tredis.call('EXPIREAT', KEYS[1], ARGV[2]);\n" +
                "\t\treturn 1;\n" +
                "\tend;\n" +
                "\tif (redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1) then\n" +
                "\t\tredis.call('HINCRBY', KEYS[1], ARGV[1], 1);\n" +
                "\t\tredis.call('EXPIREAT', KEYS[1], ARGV[2]);\n" +
                "\t\treturn 1;\n" +
                "\tend;\n" +
                "\treturn 0;\n" +
                "end;\n" +
                "return 0;";
        List<String> keys = Lists.newArrayList(this.getLockKey());
        List<String> args = Lists.newArrayList(this.getLockVal(), String.valueOf(this.getRedisKeyExpireTs() / 1000));
        return jedis.eval(luaScript, keys, args).equals(1L);
    }

    @Override
    public boolean unLockInRedis(Jedis jedis) {
        String luaScript = "local mode = redis.call('HGET', KEYS[1], 'mode');\n" +
                "if (mode == false) then\n" +
                "\treturn 0;\n" +
                "end;\n" +
                "if (mode == 'write' and redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1) then\n" +
                "\tif (redis.call('HINCRBY', KEYS[1], ARGV[1], -1) <= 0) then\n" +
                "\t\tredis.call('HDEL', KEYS[1], ARGV[1]);\n" +
                "\t\tif (redis.call('HINCRBY', KEYS[1], 'count', -1) <= 0) then\n" +
                "\t\t\tredis.call('DEL', KEYS[1]);\n" +
                "\t\tend;\n" +
                "\tend;\n" +
                "\treturn 1;\n" +
                "end;\n" +
                "return 0;";
        List<String> keys = Lists.newArrayList(this.getLockKey());
        List<String> args = Lists.newArrayList(this.getLockVal());
        return jedis.eval(luaScript, keys, args).equals(1L);
    }

    @Override
    public boolean rechargeInRedis(Jedis jedis, long expireTimeout) {
        if (this.isReleased()) {
            return true;
        }
        this.rechargeCount++;
        this.setRedisKeyExpireTs(this.getRedisKeyExpireTs() + expireTimeout);
        String luaScript = "if (redis.call('HGET', KEYS[1], 'mode') == 'write' and redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1) then\n" +
                "\treturn redis.call('EXPIREAT', KEYS[1], ARGV[2]);\n" +
                "end;\n" +
                "return 0;";
        List<String> keys = Lists.newArrayList(this.getLockKey());
        List<String> args = Lists.newArrayList(this.getLockVal(), String.valueOf(this.getRedisKeyExpireTs() / 1000));
        return jedis.eval(luaScript, keys, args).equals(1L);
    }
}

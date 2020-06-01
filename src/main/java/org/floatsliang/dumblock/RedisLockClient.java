package org.floatsliang.dumblock;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author floatsliang@gmail.com
 * @date 2020/5/11 4:39 PM
 */
public class RedisLockClient implements LockClient {

    private JedisPool jedisPool;
    private LockTimeoutRecharger recharger;
    private long pollInterval;
    private long expireTimeout;

    RedisLockClient(JedisPool jedisPool, long expireTimeout, long pollInterval) {
        this.jedisPool = jedisPool;
        this.pollInterval = pollInterval;
        this.expireTimeout = expireTimeout;
        this.recharger = new LockTimeoutRecharger(jedisPool.getResource(), expireTimeout);
        this.recharger.start();
    }

    @Override
    public boolean attemptLock(LockInfo lockInfo) {
        RedisLockInfo redisLockInfo = (RedisLockInfo) lockInfo;
        boolean hasTheLock = false;
        try (Jedis jedis = this.jedisPool.getResource()) {
            while (!hasTheLock) {
                if (redisLockInfo.isTimeout()) {
                    break;
                } else {
                    if (redisLockInfo.lockInRedis(jedis)) {
                        hasTheLock = true;
                        this.recharger.addLockToCharge(redisLockInfo);
                    } else {
                        try {
                            Thread.sleep(this.pollInterval);
                        } catch (InterruptedException ignored) {
                        }
                    }
                }
            }
        }
        return hasTheLock;
    }

    @Override
    public boolean releaseLock(LockInfo lockInfo) {
        RedisLockInfo redisLockInfo = (RedisLockInfo) lockInfo;
        // mark lock as released to remove it from LockTimeoutRecharger
        redisLockInfo.release();
        try (Jedis jedis = this.jedisPool.getResource()) {
            return redisLockInfo.unLockInRedis(jedis);
        }
    }

    @Override
    public LockInfo getLockInfo(DistributedReentrantLock lock, long timeout) {
        return new RedisLockInfo(lock.getName(), lock.getLockVal(), timeout, this.expireTimeout);
    }

    @Override
    public LockInfo getLockInfo(DistributedWriteLock lock, long timeout) {
        return new RedisWriteLockInfo(lock.getName(), lock.getLockVal(), timeout, this.expireTimeout);
    }

    @Override
    public LockInfo getLockInfo(DistributedReadLock lock, long timeout) {
        return new RedisReadLockInfo(lock.getName(), lock.getLockVal(), timeout, this.expireTimeout);
    }

    @Override
    public LockInfo getLockInfo(DistributedSemaphore lock, long timeout) {
        return new RedisSemaphoreInfo(lock.getName(), lock.getLockVal(),
                lock.getMaxPermits(), timeout, this.expireTimeout);
    }

    @Override
    public void close() {
        this.recharger.shutdown();
        this.jedisPool.close();
    }


    public static class Builder {
        private JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        private String redisHost = "localhost";
        private int redisPort = 6379;
        private int redisTimeout = 1800;
        private String redisPassword;
        private long expireTimeout = 30000;
        private long pollInterval = 1000;

        public Builder() {
        }

        public Builder withRedisHost(String host) {
            this.redisHost = host;
            return this;
        }

        public Builder withRedisPort(int port) {
            this.redisPort = port;
            return this;
        }

        public Builder withRedisTimeout(int timeout) {
            this.redisTimeout = timeout;
            return this;
        }

        public Builder withRedisPassword(String password) {
            this.redisPassword = password;
            return this;
        }

        public Builder withExpireTimeout(long expireTimeout) {
            this.expireTimeout = Math.max(expireTimeout, 3000);
            return this;
        }

        public Builder withPollInterval(long pollInterval) {
            this.pollInterval = pollInterval;
            return this;
        }

        public Builder withMaxIdle(int maxIdle) {
            this.jedisPoolConfig.setMaxIdle(maxIdle);
            return this;
        }

        public Builder withMaxWaitMills(long maxWaitMills) {
            this.jedisPoolConfig.setMaxWaitMillis(maxWaitMills);
            return this;
        }

        public Builder withTestOnBorrow(boolean testOnBorrow) {
            this.jedisPoolConfig.setTestOnBorrow(testOnBorrow);
            return this;
        }

        public Builder withTestOnReturn(boolean testOnReturn) {
            this.jedisPoolConfig.setTestOnReturn(testOnReturn);
            return this;
        }

        public RedisLockClient build() {
            JedisPool jedisPool = null;
            if (null != this.redisPassword) {
                jedisPool = new JedisPool(this.jedisPoolConfig, this.redisHost,
                        this.redisPort, this.redisTimeout, this.redisPassword);
            } else {
                jedisPool = new JedisPool(this.jedisPoolConfig, this.redisHost,
                        this.redisPort, this.redisTimeout);
            }
            return new RedisLockClient(jedisPool, this.expireTimeout, this.pollInterval);
        }
    }
}
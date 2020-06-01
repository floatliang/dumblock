package org.floatsliang.dumblock;

import java.io.Closeable;
import java.io.IOException;
import java.util.UUID;

/**
 * @author floatsliang@gmail.com
 * @date 2020/5/13 11:48 AM
 */
public class DistributedLockFactory implements Closeable {

    private LockClient lockClient;

    DistributedLockFactory(LockClient lockClient) {
        this.lockClient = lockClient;
    }

    public DistributedReentrantLock createReentrantLock(String lockKey) {
        return new DistributedReentrantLock(this.lockClient, lockKey, UUID.randomUUID());
    }

    public DistributedMutex createMutex(String lockKey) {
        return new DistributedMutex(this.lockClient, lockKey, UUID.randomUUID());
    }

    public DistributedRWLock createRWLock(String lockKey) {
        return new DistributedRWLock(this.lockClient, lockKey, UUID.randomUUID());
    }

    public DistributedSemaphore createSemaphore(String lockKey, int permits) {
        return new DistributedSemaphore(this.lockClient, lockKey, permits, UUID.randomUUID());
    }

    @Override
    public void close() throws IOException {
        this.lockClient.close();
    }

    public static class Builder {

        private RedisLockClient.Builder redisBuilder;

        public Builder withRedisClient(RedisLockClient.Builder builder) {
            this.redisBuilder = builder;
            return this;
        }

        public DistributedLockFactory build() {
            return new DistributedLockFactory(this.redisBuilder.build());
        }

    }
}

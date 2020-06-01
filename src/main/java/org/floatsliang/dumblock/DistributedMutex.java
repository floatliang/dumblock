package org.floatsliang.dumblock;

import java.io.IOException;
import java.util.UUID;

/**
 * @author floatsliang@gmail.com
 * @date 2020/5/11 4:36 PM
 */
public class DistributedMutex extends DistributedReentrantLock {

    public DistributedMutex(LockClient client, String lockKey, UUID uuid) {
        super(client, lockKey, uuid);
    }

    @Override
    public String getName() {
        return "mars-mutex-" + this.lockKey;
    }

    @Override
    public void acquire() throws IOException {
        if (!this.internalLock(-1L, false)) {
            throw new IOException(String.format("Failed to lock on %s, " +
                    "it may caused by network problem", lockKey));
        }
    }

    @Override
    public void acquire(long timeout) throws InterruptedException {
        if (!this.internalLock(timeout, false)) {
            throw new InterruptedException(String.format("Failed to lock on %s, " +
                    "it may exceed timeout limitation", lockKey));
        }
    }
}

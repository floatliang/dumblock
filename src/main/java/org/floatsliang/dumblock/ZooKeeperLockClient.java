package org.floatsliang.dumblock;

import java.io.IOException;

/**
 * @author floatsliang@gmail.com
 * @date 2020/5/11 5:15 PM
 */
public class ZooKeeperLockClient implements LockClient {

    @Override
    public boolean attemptLock(LockInfo lockInfo) {
        return false;
    }

    @Override
    public boolean releaseLock(LockInfo lockInfo) {
        return false;
    }

    @Override
    public LockInfo getLockInfo(DistributedReentrantLock lock, long timeout) {
        return null;
    }

    @Override
    public LockInfo getLockInfo(DistributedWriteLock lock, long timeout) {
        return null;
    }

    @Override
    public LockInfo getLockInfo(DistributedReadLock lock, long timeout) {
        return null;
    }

    @Override
    public LockInfo getLockInfo(DistributedSemaphore lock, long timeout) {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}

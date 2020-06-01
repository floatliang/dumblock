package org.floatsliang.dumblock;

import java.io.Closeable;

/**
 * @author floatsliang@gmail.com
 * @date 2020/5/11 4:16 PM
 */
public interface LockClient extends Closeable {

    boolean attemptLock(LockInfo lockInfo);

    boolean releaseLock(LockInfo lockInfo);

    LockInfo getLockInfo(DistributedReentrantLock lock, long timeout);

    LockInfo getLockInfo(DistributedWriteLock lock, long timeout);

    LockInfo getLockInfo(DistributedReadLock lock, long timeout);

    LockInfo getLockInfo(DistributedSemaphore lock, long timeout);
}

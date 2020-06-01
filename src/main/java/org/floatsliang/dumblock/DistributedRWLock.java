package org.floatsliang.dumblock;

import java.util.UUID;

/**
 * @author floatsliang@gmail.com
 * @date 2020/5/11 4:37 PM
 */
public class DistributedRWLock {

    private DistributedReadLock readLock;
    private DistributedWriteLock writeLock;

    public DistributedRWLock(LockClient lockClient, String lockKey, UUID uuid) {
        this.readLock = new DistributedReadLock(lockClient, lockKey, uuid);
        this.writeLock = new DistributedWriteLock(lockClient, lockKey, uuid);
    }

    public DistributedReadLock readLock() {return this.readLock;}

    public DistributedWriteLock writeLock() {return this.writeLock;}

}

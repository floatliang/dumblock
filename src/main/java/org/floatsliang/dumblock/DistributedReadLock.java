package org.floatsliang.dumblock;

import java.util.UUID;

/**
 * @author floatsliang@gmail.com
 * @date 2020/5/14 2:29 PM
 */
public class DistributedReadLock extends DistributedReentrantLock {

    public DistributedReadLock(LockClient client, String lockKey, UUID uuid) {
        super(client, lockKey, uuid);
    }

    @Override
    public String getName() {
        return "mars-rwlock-" + this.lockKey;
    }

    @Override
    boolean internalLock(long timeout, boolean allowReentry) {
        Thread currentThread = Thread.currentThread();
        LockData lockData = this.threadLockMap.get(currentThread);
        if (null != lockData) {
            lockData.inc();
            return true;
        } else {
            LockInfo lockInfo = this.client.getLockInfo(this, timeout);
            if (this.client.attemptLock(lockInfo)) {
                this.threadLockMap.put(currentThread, new LockData(currentThread, lockInfo));
                return true;
            }
        }
        return false;
    }

    @Override
    void internalUnLock(Thread unLockThread) {
        this.threadLockMap.remove(unLockThread);
    }

}

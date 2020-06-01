package org.floatsliang.dumblock;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author floatsliang@gmail.com
 * @date 2020/5/11 4:34 PM
 * Distributed impl of {@link java.util.concurrent.locks.ReentrantLock}
 */
public class DistributedReentrantLock implements DistributedLock{

    protected String lockKey;
    protected LockClient client;
    protected UUID uuid;
    protected ConcurrentHashMap<Thread, LockData> threadLockMap = new ConcurrentHashMap<>();

    public DistributedReentrantLock(LockClient client, String lockKey, UUID uuid) {
        this.client = client;
        this.lockKey = lockKey;
        this.uuid = uuid;
    }

    public String getName() {
        return "mars-lock-" + this.lockKey;
    }

    public String getLockVal() {
        return String.format("%s:Thread-%s", this.uuid.toString(), Thread.currentThread().getId());
    }

    @Override
    public void acquire() throws IOException {
        if (!this.internalLock(-1L, true)) {
            throw new IOException(String.format("Failed to lock on %s, " +
                    "it may caused by network problem", lockKey));
        }
    }

    @Override
    public void acquire(long timeout) throws InterruptedException {
        if (!this.internalLock(timeout, true)) {
            throw new InterruptedException(String.format("Failed to lock on %s, " +
                    "it may exceed timeout limitation", lockKey));
        }
    }

    @Override
    public void release() {
        Thread currentThread = Thread.currentThread();
        LockData lockData = this.threadLockMap.get(currentThread);
        if (null != lockData) {
            lockData.dec();
            if (lockData.isExpired()) {
                this.client.releaseLock(lockData.lockKey);
                this.internalUnLock(currentThread);
            }
        } else {
            throw new IllegalMonitorStateException(String.format("Failed to release lock %s, " +
                    "as this lock is not held by thread %s", lockKey, currentThread.getName()));
        }
    }

    @Override
    public boolean isAcquiredInThisProcess() {
        return this.threadLockMap.size() > 0;
    }

    @Override
    public boolean isAcquiredInThisThread() {
        return this.threadLockMap.containsKey(Thread.currentThread());
    }

    boolean internalLock(long timeout, boolean allowReentry) {
        Thread currentThread = Thread.currentThread();
        LockData lockData = this.threadLockMap.get(currentThread);
        if (null != lockData) {
            if (allowReentry) {
                lockData.inc();
                return true;
            } else {
                throw new IllegalMonitorStateException(String.format("%s is not allowed to lock twice",
                        this.getClass().getSimpleName()));
            }
        } else {
            if (this.isAcquiredInThisProcess()) {
                long startMillis = System.currentTimeMillis();
                this.internalWaitUnLock(timeout);
                if (timeout >= 0) {
                    timeout = timeout - (System.currentTimeMillis() - startMillis);
                    if (timeout <= 0) {
                        return false;
                    }
                }
                LockInfo lockInfo = this.client.getLockInfo(this, timeout);
                boolean locked = this.client.attemptLock(lockInfo);
                if (locked) {
                    this.threadLockMap.put(currentThread, new LockData(currentThread, lockInfo));
                    return true;
                }
            } else {
                LockInfo lockInfo = this.client.getLockInfo(this, timeout);
                if (this.client.attemptLock(lockInfo)) {
                    this.threadLockMap.put(currentThread, new LockData(currentThread, lockInfo));
                    return true;
                }
            }
        }
        return false;
    }

    synchronized void internalUnLock(Thread unLockThread) {
        this.threadLockMap.remove(unLockThread);
        this.notifyAll();
    }

    synchronized boolean internalWaitUnLock(long timeout) {
        if (!this.isAcquiredInThisProcess()) {
            return true;
        }
        try {
            if (timeout < 0) {
                this.wait();
            } else {
                this.wait(timeout);
            }
        } catch (InterruptedException ignored) {}
        return this.isAcquiredInThisProcess();
    }

    @Override
    public void close() {
        this.release();
    }

    static class LockData {
        final Thread owningThread;
        final LockInfo lockKey;
        final AtomicInteger lockCount;

        public LockData(Thread owningThread, LockInfo lockKey) {
            this.lockCount = new AtomicInteger(1);
            this.owningThread = owningThread;
            this.lockKey = lockKey;
        }

        public int inc() {
            return this.lockCount.incrementAndGet();
        }

        public int dec() {
            return this.lockCount.decrementAndGet();
        }

        public boolean isExpired() {
            if (this.lockCount.get() > 0) {
                return false;
            } else if (this.lockCount.get() == 0) {
                return true;
            } else {
                throw new IllegalMonitorStateException("Cannot release expired lock");
            }
        }
    }
}

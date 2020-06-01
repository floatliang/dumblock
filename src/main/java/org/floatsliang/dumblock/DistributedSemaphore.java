package org.floatsliang.dumblock;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author floatsliang@gmail.com
 * @date 2020/5/20 4:00 PM
 */
public class DistributedSemaphore implements DistributedLock {

    private int permits;
    private LockClient client;
    private String lockKey;
    private UUID uuid;
    private ConcurrentHashMap<Thread, RedisSemaphoreInfo> threadLockMap = new ConcurrentHashMap<>();

    public DistributedSemaphore(LockClient client, String lockKey, int permits, UUID uuid) {
        this.client = client;
        this.lockKey = lockKey;
        this.permits = permits;
        this.uuid = uuid;
    }

    public int getMaxPermits() {
        return this.permits;
    }

    public String getName() {
        return String.format("mars-sem-%s-%s", this.lockKey, this.permits);
    }

    public String getLockVal() {
        return String.format("%s:Thread-%s", this.uuid.toString(), Thread.currentThread().getId());
    }

    @Override
    public void acquire() throws IOException {
        this.acquire(1);
    }

    @Override
    public void acquire(long timeout) throws InterruptedException {
        this.acquire(1, timeout);
    }

    public void acquire(int permits) throws IOException {
        if (this.internalLock(permits, -1L)) {
            throw new IOException(String.format("Failed to lock on %s, " +
                    "it may caused by network problem", lockKey));
        }
    }

    public void acquire(int permits, long timeout) throws InterruptedException{
        if (this.internalLock(permits, timeout)) {
            throw new InterruptedException(String.format("Failed to lock on %s, " +
                    "it may exceed timeout limitation", lockKey));
        }
    }

    @Override
    public void release() {
    }

    public void release(int permits) {
        if (permits < 0) {
            throw new IllegalArgumentException("Cannot release negative semaphore permits: " + permits);
        }
        Thread currentThread = Thread.currentThread();
        RedisSemaphoreInfo lockData = this.threadLockMap.get(currentThread);
        if (null != lockData) {
            lockData.allocatePermits(-permits);
            this.client.releaseLock(lockData);
            if (lockData.isReleased()) {
                this.threadLockMap.remove(currentThread);
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

    @Override
    public void close() {
        Thread currentThread = Thread.currentThread();
        RedisSemaphoreInfo lockData = this.threadLockMap.get(currentThread);
        if (null != lockData) {
            this.release(lockData.allocatedPermits());
        }
    }

    boolean internalLock(int permits, long timeout) {
        if (permits < 0) {
            throw new IllegalArgumentException("Cannot acquire negative semaphore permits: " + permits);
        }
        Thread currentThread = Thread.currentThread();
        RedisSemaphoreInfo lockData = this.threadLockMap.get(currentThread);
        if (null != lockData) {
            lockData.allocatePermits(permits);
            boolean success = false;
            try {
                success = this.client.attemptLock(lockData);
            } catch (Exception ignored) { }
            if (success) {
                return true;
            } else {
                lockData.allocatePermits(-permits);
                return false;
            }
        } else {
            lockData = (RedisSemaphoreInfo) this.client.getLockInfo(this, timeout);
            lockData.allocatePermits(permits);
            if (this.client.attemptLock(lockData)) {
                this.threadLockMap.put(currentThread, lockData);
                return true;
            } else {
                return false;
            }
        }
    }
}

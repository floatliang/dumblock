package org.floatsliang.dumblock;

/**
 * @author floatsliang@gmail.com
 * @date 2020/5/11 4:21 PM
 */
public class LockInfo {

    private String lockKey;

    private long lockTimeoutTs;

    private volatile boolean released = false;

    public LockInfo(String lockKey, long timeout) {
        this.lockKey = lockKey;
        this.lockTimeoutTs = timeout < 0 ? -1L : System.currentTimeMillis() + timeout;
    }

    public String getLockKey() {
        return lockKey;
    }

    public void setLockKey(String lockKey) {
        this.lockKey = lockKey;
    }

    public long getLockTimeoutTs() {
        return lockTimeoutTs;
    }

    public void setLockTimeoutTs(long lockTimeoutTs) {
        this.lockTimeoutTs = lockTimeoutTs;
    }

    public boolean isTimeout() {
        if (this.lockTimeoutTs < 0) {
            return false;
        }
        return System.currentTimeMillis() > lockTimeoutTs;
    }

    public void release() {
        this.released = true;
    }

    public boolean isReleased() {
        return this.released;
    }
}

package org.floatsliang.dumblock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * @author floatsliang@gmail.com
 * @date 2020/5/12 12:27 PM
 */
public class LockTimeoutRecharger extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(LockTimeoutRecharger.class);
    // update redis key expire ts 500ms before it expired
    private static final long SAFETY_TIME_OFFSET = 1000;
    // default redis key expire timeout
    private static final long DEFAULT_EXPIRE_TIMEOUT = 30000;
    // max recharge times
    private static final int DEAD_LOCK_THRESHOLD = 60;

    private PriorityBlockingQueue<RedisLockInfo> liveLocksToTimeout = new PriorityBlockingQueue<>(10,
            Comparator.comparingLong(RedisLockInfo::getLockTimeoutTs));
    private Jedis jedis;
    // redis key expire timeout
    private long expireTimeout;
    private int maxRechargeThreshold;
    private volatile boolean isShutdown = false;

    public LockTimeoutRecharger(Jedis jedis) {
        this(jedis, DEFAULT_EXPIRE_TIMEOUT, DEAD_LOCK_THRESHOLD);
    }

    public LockTimeoutRecharger(Jedis jedis, long expireTimeout) {this(jedis, expireTimeout, DEAD_LOCK_THRESHOLD);}

    public LockTimeoutRecharger(Jedis jedis, long expireTimeout, int maxRechargeThreshold) {
        this.jedis = jedis;
        this.expireTimeout = Math.max(expireTimeout, 3000);
        this.maxRechargeThreshold = maxRechargeThreshold;
        this.setName("LockTimeoutRecharger-" + this.getId());
        this.setDaemon(true);
    }

    public boolean addLockToCharge(RedisLockInfo lockInfo) {
        if (lockInfo.isExpired() || lockInfo.isReleased()) {
            return false;
        }
        if (!this.liveLocksToTimeout.contains(lockInfo)) {
            this.liveLocksToTimeout.add(lockInfo);
            // notify recharger there is a new lock to recharge
            if (Thread.currentThread() != this && this.liveLocksToTimeout.peek() == lockInfo) {
                this.interrupt();
            }
            return true;
        } else {
            return false;
        }
    }

    public void shutdown() {
        this.isShutdown = true;
        this.jedis.close();
    }

    private boolean isReadyToRecharge(RedisLockInfo lockInfo) {
        return System.currentTimeMillis() > lockInfo.getRedisKeyExpireTs() - SAFETY_TIME_OFFSET;
    }

    @Override
    public void run() {
        while (!this.isShutdown) {
            RedisLockInfo lockInfo = null;
            try {
                lockInfo = this.liveLocksToTimeout.take();
            } catch (InterruptedException ignored) {
            }
            if (null == lockInfo) {
                continue;
            }
            if (lockInfo.isExpired()) {
                logger.warn("Failed to recharge Redis lock {} expire ts, " +
                        "as it is already expired", lockInfo.getLockKey());
                continue;
            } else if (lockInfo.isReleased()) {
                continue;
            }
            if (lockInfo.getRechargeCount() >= this.maxRechargeThreshold) {
                logger.error("Dead lock detected, lock {} exceed max lock threshold: recharged {} times, " +
                                "locked {} ms, forcing it to expire in redis side", lockInfo.getLockKey(),
                        this.maxRechargeThreshold, this.maxRechargeThreshold * this.expireTimeout);
                continue;
            }
            if (this.isReadyToRecharge(lockInfo)) {
                if (lockInfo.rechargeInRedis(jedis, this.expireTimeout)) {
                    this.addLockToCharge(lockInfo);
                } else {
                    logger.warn("Failed to recharge Redis lock {} expire ts, " +
                            "as it may already expired in redis side", lockInfo.getLockKey());
                }
            } else {
                try {
                    long timeToSleep = Math.max(
                            lockInfo.getRedisKeyExpireTs() - SAFETY_TIME_OFFSET - System.currentTimeMillis(), 100);
                    Thread.sleep(timeToSleep);
                } catch (InterruptedException ignored) {}
                if (this.isReadyToRecharge(lockInfo)) {
                    if (lockInfo.rechargeInRedis(jedis, this.expireTimeout)) {
                        this.addLockToCharge(lockInfo);
                    } else {
                        logger.warn("Failed to recharge Redis lock {} expire ts, " +
                                "as it may already expired in redis side", lockInfo.getLockKey());
                    }
                } else {
                    this.addLockToCharge(lockInfo);
                }
            }
        }
    }
}

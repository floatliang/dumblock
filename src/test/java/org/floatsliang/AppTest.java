package org.floatsliang;

import static org.junit.Assert.assertTrue;

import org.floatsliang.dumblock.*;
import org.junit.Test;

import java.io.IOException;

/**
 * Unit test for dumblock.
 */
public class AppTest 
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void testRWLock() throws IOException {
        DistributedLockFactory lockFactory = new DistributedLockFactory.Builder().withRedisClient(
                new RedisLockClient.Builder().withRedisHost("localhost").withRedisPort(6379).withExpireTimeout(10000)).build();
        DistributedRWLock lock = lockFactory.createRWLock("lala");
        DistributedWriteLock writeLock = lock.writeLock();
        System.out.println("try lock");
        writeLock.acquire();
        System.out.println("get lock");
        writeLock.release();
        DistributedReadLock readLock = lock.readLock();
        readLock.acquire();
        readLock.release();
    }
}

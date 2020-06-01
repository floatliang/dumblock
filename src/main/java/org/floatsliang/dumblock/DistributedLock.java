package org.floatsliang.dumblock;

import java.io.IOException;

/**
 * @author floatsliang@gmail.com
 * @date 2020/5/11 4:29 PM
 */
public interface DistributedLock extends AutoCloseable {

    void acquire() throws IOException;

    void acquire(long timeout) throws InterruptedException;

    void release();

    boolean isAcquiredInThisProcess();

    boolean isAcquiredInThisThread();
}

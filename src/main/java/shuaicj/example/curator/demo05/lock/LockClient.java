package shuaicj.example.curator.demo05.lock;

import java.util.concurrent.BlockingQueue;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

/**
 * Distributed lock.
 *
 * @author shuaicj 2019/03/06
 */
@Slf4j
public class LockClient {

    private final String name;
    private final InterProcessMutex mutex;

    public LockClient(String name, CuratorFramework client, String path) {
        this.name = name;
        this.mutex = new InterProcessMutex(client, path);
    }

    public void acquire(BlockingQueue<String> lockHolder) throws Exception {
        mutex.acquire();
        log.info("{} got the lock", name);
        lockHolder.offer(name);
    }

    public void release() throws Exception {
        mutex.release();
    }
}

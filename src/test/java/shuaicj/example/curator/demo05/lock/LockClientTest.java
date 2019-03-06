package shuaicj.example.curator.demo05.lock;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import shuaicj.example.curator.demo01.connect.Client;
import shuaicj.example.curator.demo02.basic.ops.BasicOps;

/**
 * Test {@link LockClient}.
 *
 * @author shuaicj 2019/03/06
 */
public class LockClientTest {

    private static TestingServer server;
    private static CuratorFramework client;
    private static BasicOps basicOps;

    @BeforeClass
    public static void init() throws Exception {
        server = new TestingServer();
        server.start();
        client = Client.get(server.getConnectString());
        client.start();
        basicOps = new BasicOps(client);
    }

    @AfterClass
    public static void close() throws Exception {
        basicOps.delete("/demo05");
        client.close();
        server.close();
    }

    @Test
    public void testDistributedLock() throws Exception {
        List<String> names = new ArrayList<>();
        BlockingQueue<String> lockHolder = new LinkedBlockingQueue<>();
        List<Exception> exceptionHolder = Collections.synchronizedList(new ArrayList<>());
        ExecutorService threadPool = Executors.newFixedThreadPool(3);

        for (int i = 0; i < 3; i++) {
            String name = "contender" + i;
            names.add(name);
            threadPool.submit(() -> {
                LockClient lockClient = new LockClient(name, client, "/demo05/lock");
                try {
                    lockClient.acquire(lockHolder);
                    TimeUnit.MILLISECONDS.sleep(1000); // do work
                } catch (Exception e) {
                    exceptionHolder.add(e);
                } finally {
                    try {
                        lockClient.release();
                    } catch (Exception e) {
                        exceptionHolder.add(e);
                    }
                }
            });
        }

        while (!names.isEmpty()) {
            String name = lockHolder.take();
            assertThat(name).isIn(names);
            assertThat(lockHolder).isEmpty();
            names.remove(name);
        }
        assertThat(exceptionHolder).isEmpty();

        threadPool.shutdown();
        threadPool.awaitTermination(3, TimeUnit.MINUTES);
    }

}
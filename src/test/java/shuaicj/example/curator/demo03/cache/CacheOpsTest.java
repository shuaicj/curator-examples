package shuaicj.example.curator.demo03.cache;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import shuaicj.example.curator.demo01.connect.Client;
import shuaicj.example.curator.demo02.basic.ops.BasicOps;

/**
 * Test {@link CacheOps}.
 *
 * @author shuaicj 2019/01/18
 */
public class CacheOpsTest {

    private static TestingServer server;
    private static CuratorFramework client;
    private static BasicOps basicOps;
    private static CacheOps cacheOps;

    @BeforeClass
    public static void init() throws Exception {
        server = new TestingServer();
        server.start();
        client = Client.get(server.getConnectString());
        client.start();
        basicOps = new BasicOps(client);
        cacheOps = new CacheOps(client);
    }

    @AfterClass
    public static void close() throws Exception {
        basicOps.delete("/demo03");
        client.close();
        server.close();
    }

    @Test
    public void newNodeCache() throws Exception {
        String path = "/demo03";
        BlockingQueue<String> dataHolder = new LinkedBlockingQueue<>();
        NodeCache cache = cacheOps.newNodeCache(path, dataHolder::offer);
        assertThat(cache.getCurrentData()).isNull();

        basicOps.create(path, "abc");
        assertThat(dataHolder.take()).isEqualTo("abc");

        basicOps.setData(path, "def");
        assertThat(dataHolder.take()).isEqualTo("def");

        basicOps.setData(path, "def");
        assertThat(dataHolder.take()).isEqualTo("def");

        cache.close();
    }
}
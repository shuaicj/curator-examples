package shuaicj.example.curator.demo03.cache;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import shuaicj.example.curator.demo01.connect.Client;
import shuaicj.example.curator.demo02.basic.ops.BasicOps;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test {@link CacheOps}.
 *
 * @author shuaicj 2019/02/10
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
        String path = "/demo03/node";
        BlockingQueue<Optional<String>> dataHolder = new LinkedBlockingQueue<>();
        NodeCache cache = cacheOps.newNodeCache(path, dataHolder::offer);
        assertThat(cache.getCurrentData()).isNull();

        basicOps.create(path, "abc");
        assertThat(dataHolder.take()).isPresent().contains("abc");

        basicOps.setData(path, "def");
        assertThat(dataHolder.take()).isPresent().contains("def");

        basicOps.setData(path, "def");
        assertThat(dataHolder.take()).isPresent().contains("def");

        basicOps.delete(path);
        assertThat(dataHolder.take()).isNotPresent();

        cache.close();
    }

    @Test
    public void newPathChildrenCache() throws Exception {
        basicOps.create("/demo03/child/abc", "abc");
        basicOps.create("/demo03/child/def", "def");
        BlockingQueue<PathChildrenCacheEvent> eventHolder = new LinkedBlockingQueue<>();
        PathChildrenCache cache = cacheOps.newPathChildrenCache("/demo03/child", eventHolder::offer);

        List<ChildData> curData = new ArrayList<>(cache.getCurrentData());
        curData.sort(Comparator.comparing(ChildData::getPath));
        assertThat(curData).hasSize(2);
        assertThat(curData.get(0).getPath()).isEqualTo("/demo03/child/abc");
        assertThat(curData.get(0).getData()).isEqualTo("abc".getBytes());
        assertThat(curData.get(1).getPath()).isEqualTo("/demo03/child/def");
        assertThat(curData.get(1).getData()).isEqualTo("def".getBytes());

        basicOps.create("/demo03/child/ghi", "ghi");
        PathChildrenCacheEvent event = eventHolder.take();
        assertThat(event.getType()).isEqualTo(PathChildrenCacheEvent.Type.CHILD_ADDED);
        assertThat(event.getData().getPath()).isEqualTo("/demo03/child/ghi");
        assertThat(event.getData().getData()).isEqualTo("ghi".getBytes());

        basicOps.setData("/demo03/child/def", "xxx");
        event = eventHolder.take();
        assertThat(event.getType()).isEqualTo(PathChildrenCacheEvent.Type.CHILD_UPDATED);
        assertThat(event.getData().getPath()).isEqualTo("/demo03/child/def");
        assertThat(event.getData().getData()).isEqualTo("xxx".getBytes());

        basicOps.delete("/demo03/child/abc");
        event = eventHolder.take();
        assertThat(event.getType()).isEqualTo(PathChildrenCacheEvent.Type.CHILD_REMOVED);
        assertThat(event.getData().getPath()).isEqualTo("/demo03/child/abc");

        cache.close();
    }

    @Test
    public void newTreeCache() throws Exception {
        // basicOps.create("/demo03/tree", "tree");
        basicOps.create("/demo03/tree/abc", "abc");
        basicOps.create("/demo03/tree/def", "def");
        BlockingQueue<TreeCacheEvent> eventHolder = new LinkedBlockingQueue<>();
        TreeCache cache = cacheOps.newTreeCache("/demo03/tree", eventHolder::offer);

        TreeCacheEvent event = eventHolder.take();
        assertThat(event.getType()).isEqualTo(TreeCacheEvent.Type.NODE_ADDED);
        assertThat(event.getData().getPath()).isEqualTo("/demo03/tree");
        assertThat(event.getData().getData()).isEqualTo("".getBytes());

        event = eventHolder.take();
        assertThat(event.getType()).isEqualTo(TreeCacheEvent.Type.NODE_ADDED);
        assertThat(event.getData().getPath()).isEqualTo("/demo03/tree/abc");
        assertThat(event.getData().getData()).isEqualTo("abc".getBytes());

        event = eventHolder.take();
        assertThat(event.getType()).isEqualTo(TreeCacheEvent.Type.NODE_ADDED);
        assertThat(event.getData().getPath()).isEqualTo("/demo03/tree/def");
        assertThat(event.getData().getData()).isEqualTo("def".getBytes());

        basicOps.create("/demo03/tree/abc/ghi", "ghi");
        event = eventHolder.take();
        assertThat(event.getType()).isEqualTo(TreeCacheEvent.Type.NODE_ADDED);
        assertThat(event.getData().getPath()).isEqualTo("/demo03/tree/abc/ghi");
        assertThat(event.getData().getData()).isEqualTo("ghi".getBytes());

        basicOps.setData("/demo03/tree/def", "xxx");
        event = eventHolder.take();
        assertThat(event.getType()).isEqualTo(TreeCacheEvent.Type.NODE_UPDATED);
        assertThat(event.getData().getPath()).isEqualTo("/demo03/tree/def");
        assertThat(event.getData().getData()).isEqualTo("xxx".getBytes());

        basicOps.delete("/demo03/tree/abc");
        event = eventHolder.take();
        assertThat(event.getType()).isEqualTo(TreeCacheEvent.Type.NODE_REMOVED);
        assertThat(event.getData().getPath()).isEqualTo("/demo03/tree/abc/ghi");
        event = eventHolder.take();
        assertThat(event.getType()).isEqualTo(TreeCacheEvent.Type.NODE_REMOVED);
        assertThat(event.getData().getPath()).isEqualTo("/demo03/tree/abc");

        cache.close();
    }
}
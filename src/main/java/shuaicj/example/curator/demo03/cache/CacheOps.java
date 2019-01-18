package shuaicj.example.curator.demo03.cache;

import java.util.function.Consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;

/**
 * Cache operations.
 *
 * @author shuaicj 2019/01/18
 */
@Slf4j
public class CacheOps {

    private final CuratorFramework client;

    public CacheOps(CuratorFramework client) {
        this.client = client;
    }

    public NodeCache newNodeCache(String path, Consumer<String> dataConsumer) throws Exception {
        NodeCache cache = new NodeCache(client, path);
        cache.start(true);
        cache.getListenable().addListener(() -> {
            ChildData childData = cache.getCurrentData();
            String data = new String(childData.getData());
            log.info("{} node changed to {}", childData.getPath(), data);
            dataConsumer.accept(data);
        });
        return cache;
    }
}

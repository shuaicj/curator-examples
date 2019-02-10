package shuaicj.example.curator.demo03.cache;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;

import java.util.Optional;
import java.util.function.Consumer;

/**
 * Cache operations.
 *
 * @author shuaicj 2019/02/10
 */
@Slf4j
public class CacheOps {

    private final CuratorFramework client;

    public CacheOps(CuratorFramework client) {
        this.client = client;
    }

    public NodeCache newNodeCache(String path, Consumer<Optional<String>> dataConsumer) throws Exception {
        NodeCache cache = new NodeCache(client, path);
        cache.getListenable().addListener(() -> {
            ChildData childData = cache.getCurrentData();
            if (childData != null) {
                String data = new String(childData.getData());
                log.info("{} node changed to {}", childData.getPath(), data);
                dataConsumer.accept(Optional.of(data));
            } else {
                log.info("{} node deleted", path);
                dataConsumer.accept(Optional.empty());
            }
        });
        cache.start(true);
        return cache;
    }

    public PathChildrenCache newPathChildrenCache(String path, Consumer<PathChildrenCacheEvent> eventConsumer)
            throws Exception {
        PathChildrenCache cache = new PathChildrenCache(client, path, true);
        cache.getListenable().addListener((client, event) -> {
            switch (event.getType()) {
                case CHILD_ADDED:
                case CHILD_UPDATED:
                    String data = new String(event.getData().getData());
                    log.info("{}, path: {}, data: {}", event.getType(), event.getData().getPath(), data);
                    eventConsumer.accept(event);
                    break;
                case CHILD_REMOVED:
                    log.info("{}, path: {}", event.getType(), event.getData().getPath());
                    eventConsumer.accept(event);
                    break;
                default:
                    log.info("other events received, {}", event);
            }
        });
        cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        return cache;
    }

    public TreeCache newTreeCache(String path, Consumer<TreeCacheEvent> eventConsumer)
            throws Exception {
        TreeCache cache = new TreeCache(client, path);
        cache.getListenable().addListener((client, event) -> {
            switch (event.getType()) {
                case NODE_ADDED:
                case NODE_UPDATED:
                    String data = new String(event.getData().getData());
                    log.info("{}, path: {}, data: {}", event.getType(), event.getData().getPath(), data);
                    eventConsumer.accept(event);
                    break;
                case NODE_REMOVED:
                    log.info("{}, path: {}", event.getType(), event.getData().getPath());
                    eventConsumer.accept(event);
                    break;
                default:
                    log.info("other events received, {}", event);
            }
        });
        cache.start();
        return cache;
    }
}

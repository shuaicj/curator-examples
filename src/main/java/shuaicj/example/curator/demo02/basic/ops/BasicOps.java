package shuaicj.example.curator.demo02.basic.ops;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * Basic operations.
 *
 * @author shuaicj 2019/01/05
 */
@Slf4j
public class BasicOps {

    private final CuratorFramework client;

    public BasicOps(CuratorFramework client) {
        this.client = client;
    }

    public boolean exists(String path) throws Exception {
        Stat stat = client.checkExists().forPath(path);
        return stat != null;
    }

    public String create(String path, String data) throws Exception {
        try {
            return client.create()
                         .creatingParentsIfNeeded()
                         .forPath(path, data.getBytes());
        } catch (KeeperException.NodeExistsException e) {
            log.info("{} exists already", e.getPath());
            return e.getPath();
        }
    }

    public String createEphemeral(String path, String data) throws Exception {
        try {
            return client.create()
                         .creatingParentsIfNeeded()
                         .withMode(CreateMode.EPHEMERAL)
                         .forPath(path, data.getBytes());
        } catch (KeeperException.NodeExistsException e) {
            log.info("{} exists already", e.getPath());
            return e.getPath();
        }
    }

    public String createEphemeralSequential(String path, String data) throws Exception {
        return client.create()
                     .creatingParentsIfNeeded()
                     .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                     .forPath(path, data.getBytes());
    }

    public void delete(String path) throws Exception {
        try {
            client.delete().guaranteed().deletingChildrenIfNeeded().forPath(path);
        } catch (KeeperException.NoNodeException e) {
            log.info("{} does not exist already", e.getPath());
        }
    }

    public String getData(String path) throws Exception {
        return new String(client.getData().forPath(path));
    }

    public void setData(String path, String data) throws Exception {
        client.setData().forPath(path, data.getBytes());
    }

    public List<String> getChildren(String path) throws Exception {
        return client.getChildren().forPath(path);
    }
}

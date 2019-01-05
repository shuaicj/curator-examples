package shuaicj.example.curator.demo02.basic.ops;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.data.Stat;

/**
 * Basic operations.
 *
 * @author shuaicj 2019/01/05
 */
public class BasicOps {

    private final CuratorFramework client;

    public BasicOps(CuratorFramework client) {
        this.client = client;
    }

    public boolean exists(String path) throws Exception {
        Stat stat = client.checkExists().forPath(path);
        return stat != null;
    }
}

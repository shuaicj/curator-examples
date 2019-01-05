package shuaicj.example.curator.demo01.connect;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;

/**
 * Init the curator.
 *
 * @author shuaicj 2019/01/05
 */
public class Client {

    public static CuratorFramework get() {
        int maxRetries = 3;
        int retryIntervalMillis = 1000;
        RetryPolicy retryPolicy = new RetryNTimes(maxRetries, retryIntervalMillis);

        CuratorFramework client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", retryPolicy);
        client.start();

        return client;
    }
}

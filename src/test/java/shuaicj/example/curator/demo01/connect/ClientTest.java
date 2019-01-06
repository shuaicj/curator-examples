package shuaicj.example.curator.demo01.connect;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Test {@link Client}.
 *
 * @author shuaicj 2019/01/05
 */
public class ClientTest {

    @Test
    public void get() throws Exception {
        TestingServer server = new TestingServer();
        server.start();
        CuratorFramework client = Client.get(server.getConnectString());
        client.start();
        TimeUnit.MILLISECONDS.sleep(100);
        client.close();
        server.close();
    }
}
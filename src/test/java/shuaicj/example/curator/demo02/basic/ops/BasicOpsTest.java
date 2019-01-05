package shuaicj.example.curator.demo02.basic.ops;

import org.apache.curator.framework.CuratorFramework;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import shuaicj.example.curator.demo01.connect.Client;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test {@link BasicOps}.
 *
 * @author shuaicj 2019/01/05
 */
public class BasicOpsTest {

    private static CuratorFramework client;
    private static BasicOps basicOps;

    @BeforeClass
    public static void init() {
        client = Client.get();
        basicOps = new BasicOps(client);
    }

    @AfterClass
    public static void close() {
        client.close();
    }

    @Test
    public void exists() throws Exception {
        assertThat(basicOps.exists("/")).isTrue();
        assertThat(basicOps.exists("/a/b/c/d/e")).isFalse();
    }
}
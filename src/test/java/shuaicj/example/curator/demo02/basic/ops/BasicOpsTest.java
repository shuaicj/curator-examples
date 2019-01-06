package shuaicj.example.curator.demo02.basic.ops;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
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
        basicOps.delete("/demo02");
        client.close();
        server.close();
    }

    @Test
    public void exists() throws Exception {
        assertThat(basicOps.exists("/")).isTrue();
        assertThat(basicOps.exists("/demo02/nodeNotExists")).isFalse();
    }

    @Test
    public void create() throws Exception {
        String path = "/demo02/nodeCreate";
        assertThat(basicOps.create(path, "")).isEqualTo(path);
        assertThat(basicOps.create(path, "")).isEqualTo(path);
    }

    @Test
    public void createEphemeral() throws Exception {
        String path = "/demo02/nodeCreateEphemeral";
        assertThat(basicOps.createEphemeral(path, "")).isEqualTo(path);
        assertThat(basicOps.createEphemeral(path, "")).isEqualTo(path);
    }

    @Test
    public void createEphemeralSequential() throws Exception {
        String prefix = "/demo02/nodeCreateEphemeralSequential";
        String path1 = basicOps.createEphemeralSequential(prefix, "");
        assertThat(path1).startsWith(prefix);
        String path2 = basicOps.createEphemeralSequential(prefix, "");
        assertThat(path2).startsWith(prefix);
        assertThat(Long.valueOf(path2.substring(prefix.length())))
                .isEqualTo(Long.valueOf(path1.substring(prefix.length())) + 1);
    }

    @Test
    public void delete() throws Exception {
        String path = "/demo02/nodeCreate";
        basicOps.create(path, "");
        basicOps.delete(path);
        assertThat(basicOps.exists(path)).isFalse();
        basicOps.delete(path);
        assertThat(basicOps.exists(path)).isFalse();
    }

    @Test
    public void getData() throws Exception {
        String path = "/demo02/nodeGetData";
        String data = "abc";
        basicOps.create(path, data);
        assertThat(basicOps.getData(path)).isEqualTo(data);
    }

    @Test
    public void setData() throws Exception {
        String path = "/demo02/nodeSetData";
        String data1 = "abc";
        String data2 = "def";
        basicOps.create(path, data1);
        basicOps.setData(path, data2);
        assertThat(basicOps.getData(path)).isEqualTo(data2);
    }

    @Test
    public void getChildren() throws Exception {
        String path = "/demo02/nodeGetChildren";
        basicOps.create(path + "/a", "");
        basicOps.create(path + "/b", "");
        basicOps.create(path + "/c", "");
        assertThat(basicOps.getChildren(path)).containsOnly("a", "b", "c");
    }
}
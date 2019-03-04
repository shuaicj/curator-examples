package shuaicj.example.curator.demo04.leader;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import shuaicj.example.curator.demo01.connect.Client;
import shuaicj.example.curator.demo02.basic.ops.BasicOps;

/**
 * Test {@link LeaderClient}.
 *
 * @author shuaicj 2019/03/01
 */
public class LeaderClientTest {

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
        basicOps.delete("/demo04");
        client.close();
        server.close();
    }

    @Test
    public void testLeadership() throws Exception {
        List<CuratorFramework> clients = new ArrayList<>();
        List<String> names = new ArrayList<>();
        Map<String, LeaderClient> leaderClients = new HashMap<>();
        BlockingQueue<String> leaderReceiver = new LinkedBlockingQueue<>();

        for (int i = 0; i < 3; i++) {
            CuratorFramework client = Client.get(server.getConnectString());
            client.start();
            clients.add(client);

            String name = "contender" + i;
            names.add(name);

            LeaderClient leaderClient = new LeaderClient(name, client, "/demo04/leader");
            leaderClient.start(leaderReceiver);
            leaderClients.put(name, leaderClient);
        }

        while (!names.isEmpty()) {
            String name = leaderReceiver.take();
            assertThat(name).isIn(names);
            names.remove(name);
            leaderClients.get(name).close();
        }

        for (CuratorFramework client : clients) {
            client.close();
        }
    }

}
package shuaicj.example.curator.demo04.leader;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;

/**
 * Leader election.
 *
 * @author shuaicj 2019/03/01
 */
@Slf4j
public class LeaderClient implements Closeable {

    private final String name;
    private final LeaderLatch leaderLatch;

    public LeaderClient(String name, CuratorFramework client, String path) {
        this.name = name;
        this.leaderLatch = new LeaderLatch(client, path);
    }

    public void start(BlockingQueue<String> leaderReceiver) throws Exception {
        leaderLatch.addListener(new LeaderLatchListener() {
            @Override
            public void isLeader() {
                log.info("{} take leadership", name);
                leaderReceiver.offer(name);
            }

            @Override
            public void notLeader() {
                log.info("{} release leadership", name);
            }
        });
        leaderLatch.start();
    }

    @Override
    public void close() throws IOException {
        leaderLatch.close();
    }
}

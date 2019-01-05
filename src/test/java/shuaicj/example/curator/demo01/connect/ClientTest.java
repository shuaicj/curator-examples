package shuaicj.example.curator.demo01.connect;

import org.apache.curator.framework.CuratorFramework;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test {@link Client}.
 *
 * @author shuaicj 2019/01/05
 */
public class ClientTest {

    @Test
    public void get() {
        CuratorFramework client = Client.get();
        assertThat(client).isNotNull();
        client.close();
    }
}